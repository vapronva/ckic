package watcher

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"gl.vprw.ru/vapronva/ckic/pkg/constants"
)

type ConfigHandlerFunc func(string)

type ConfigWatcher struct {
	clientset           *kubernetes.Clientset
	namespace           string
	configMapName       string
	configHandler       ConfigHandlerFunc
	lastResourceVersion string

	nodeAvailableCheck  func() bool
	isPaused            bool
	cachedConfig        string
	hasCachedConfig     bool
	lastProcessedConfig string

	failureCount int
	maxFailures  int
	resetTimeout time.Duration
	lastSuccess  time.Time
}

func NewConfigWatcher(clientset *kubernetes.Clientset, namespace, configMapName string,
	handler ConfigHandlerFunc, nodeAvailableCheck func() bool,
) *ConfigWatcher {
	return &ConfigWatcher{
		clientset:          clientset,
		namespace:          namespace,
		configMapName:      configMapName,
		configHandler:      handler,
		nodeAvailableCheck: nodeAvailableCheck,
		isPaused:           false,
		maxFailures:        5,
		resetTimeout:       5 * time.Minute,
		lastSuccess:        time.Now(),
	}
}

func (w *ConfigWatcher) Pause() {
	if !w.isPaused {
		w.isPaused = true
		log.Info().Msg("ConfigMap watcher paused")
	}
}

func (w *ConfigWatcher) Resume() {
	if w.isPaused {
		w.isPaused = false
		log.Info().Msg("ConfigMap watcher resumed")
		if w.hasCachedConfig && w.nodeAvailableCheck() {
			if w.cachedConfig != w.lastProcessedConfig {
				w.configHandler(w.cachedConfig)
				w.lastProcessedConfig = w.cachedConfig
			} else {
				log.Debug().Msg("Cached config unchanged, skipping handler on resume")
			}
			w.hasCachedConfig = false
		}
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (w *ConfigWatcher) refreshResourceVersion(ctx context.Context, logger zerolog.Logger) error {
	cm, err := w.clientset.CoreV1().ConfigMaps(w.namespace).Get(ctx, w.configMapName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	w.lastResourceVersion = cm.ResourceVersion
	logger.Info().Str("resourceVersion", w.lastResourceVersion).Msg("Resource version refreshed")
	if configData, exists := cm.Data["Caddyfile"]; exists {
		if configData == w.lastProcessedConfig {
			logger.Debug().Msg("Refreshed ConfigMap content unchanged, skipping handler")
			return nil
		}
		if w.nodeAvailableCheck() {
			logger.Info().Msg("Refreshed ConfigMap loaded, updating configuration")
			w.configHandler(configData)
			w.lastProcessedConfig = configData
			w.lastSuccess = time.Now()
		} else {
			logger.Info().Msg("Refreshed ConfigMap loaded but no eligible nodes, caching config")
			w.cachedConfig = configData
			w.hasCachedConfig = true
		}
	}
	return nil
}

func (w *ConfigWatcher) Start(ctx context.Context) {
	logger := log.With().Str("component", "config_watcher").Str("namespace", w.namespace).Str("configmap", w.configMapName).Logger()
	logger.Info().Msg("Starting config watcher")
	configMap, err := w.clientset.CoreV1().ConfigMaps(w.namespace).Get(ctx, w.configMapName, metav1.GetOptions{})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get initial ConfigMap, creating a new one")
		configMap = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      w.configMapName,
				Namespace: w.namespace,
			},
			Data: map[string]string{
				"Caddyfile": ":80 {\n    respond \"Hello, world!\"\n}\n",
			},
		}
		_, err = w.clientset.CoreV1().ConfigMaps(w.namespace).Create(ctx, configMap, metav1.CreateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create default ConfigMap")
		}
	} else {
		if configData, exists := configMap.Data["Caddyfile"]; exists {
			logger.Info().Msg("Initial ConfigMap loaded")
			if w.nodeAvailableCheck() {
				w.configHandler(configData)
				w.lastProcessedConfig = configData
			} else {
				logger.Info().Msg("No eligible nodes available, caching initial config")
				w.cachedConfig = configData
				w.hasCachedConfig = true
			}
		} else {
			logger.Warn().Msg("ConfigMap missing Caddyfile key")
		}
	}
	delay := constants.ConfigMapWatcherInitialDelay
	maxDelay := constants.ConfigMapWatcherMaxDelay
	multiplier := 1.5
	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Config watcher shutting down")
			return
		default:
		}
		if w.isPaused {
			logger.Debug().Msg("Config watcher is paused; sleeping until resumed")
			time.Sleep(10 * time.Second)
			continue
		}
		watchOptions := metav1.ListOptions{
			FieldSelector:   "metadata.name=" + w.configMapName,
			ResourceVersion: "",
		}
		watcher, err := w.clientset.CoreV1().ConfigMaps(w.namespace).Watch(ctx, watchOptions)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create ConfigMap watcher, retrying")
			w.failureCount++
			if w.failureCount >= w.maxFailures {
				sleepTime := w.resetTimeout - time.Since(w.lastSuccess)
				if sleepTime > 0 {
					logger.Warn().Msgf("Circuit breaker open, sleeping for %v", sleepTime)
					time.Sleep(sleepTime)
				}
				w.failureCount = 0
				if errRV := w.refreshResourceVersion(ctx, logger); errRV != nil {
					logger.Error().Err(errRV).Msg("Failed to refresh resource version")
				}
			} else {
				time.Sleep(delay)
				delay = minDuration(time.Duration(float64(delay)*multiplier), maxDelay)
			}
			continue
		}
		delay = constants.ConfigMapWatcherInitialDelay
		for event := range watcher.ResultChan() {
			select {
			case <-ctx.Done():
				watcher.Stop()
				logger.Info().Msg("Config watcher shutting down")
				return
			default:
			}
			if event.Type == watch.Error {
				if status, ok := event.Object.(*metav1.Status); ok && status.Code == 410 {
					logger.Info().Int32("code", status.Code).Msg("Received watch timeout (410); ignoring because we restart with an empty resourceVersion")
				} else if status, ok := event.Object.(*metav1.Status); ok {
					logger.Error().Int32("code", status.Code).Str("reason", string(status.Reason)).Str("message", status.Message).Msg("Error in ConfigMap watch channel")
				} else {
					logger.Error().Msg("Error in ConfigMap watch channel")
				}
				break
			}
			cm, ok := event.Object.(*corev1.ConfigMap)
			if !ok {
				logger.Warn().Msg("Unexpected object in ConfigMap watch")
				continue
			}
			if event.Type == watch.Added || event.Type == watch.Modified {
				if configData, exists := cm.Data["Caddyfile"]; exists {
					if configData == w.lastProcessedConfig {
						logger.Debug().Str("event", string(event.Type)).Msg("ConfigMap content unchanged, skipping handler")
						continue
					}
					if w.nodeAvailableCheck() {
						logger.Info().Str("event", string(event.Type)).
							Msg("ConfigMap updated, notifying handlers")
						w.configHandler(configData)
						w.lastProcessedConfig = configData
						w.lastSuccess = time.Now()
						w.failureCount = 0
					} else {
						logger.Info().Msg("ConfigMap updated but no eligible nodes available, caching config")
						w.cachedConfig = configData
						w.hasCachedConfig = true
						w.Pause()
					}
				} else {
					logger.Warn().Msg("Updated ConfigMap missing Caddyfile key")
				}
			}
		}
		logger.Info().Msg("ConfigMap watch channel closed, restarting")
		time.Sleep(delay)
		delay = minDuration(time.Duration(float64(delay)*multiplier), maxDelay)
	}
}
