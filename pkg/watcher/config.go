package watcher

import (
	"context"
	"time"

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

	nodeAvailableCheck func() bool
	isPaused           bool
	cachedConfig       string
	hasCachedConfig    bool

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
			w.configHandler(w.cachedConfig)
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

func (w *ConfigWatcher) Start(ctx context.Context) {
	logger := log.With().
		Str("component", "config_watcher").
		Str("namespace", w.namespace).
		Str("configmap", w.configMapName).
		Logger()
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
		configMap, err = w.clientset.CoreV1().ConfigMaps(w.namespace).Create(ctx, configMap, metav1.CreateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create default ConfigMap")
		}
	} else {
		if caddyfileData, exists := configMap.Data["Caddyfile"]; exists {
			logger.Info().Msg("Initial ConfigMap loaded")
			if w.nodeAvailableCheck() {
				w.configHandler(caddyfileData)
			} else {
				logger.Info().Msg("No eligible nodes available, caching initial config")
				w.cachedConfig = caddyfileData
				w.hasCachedConfig = true
			}
		} else {
			logger.Warn().Msg("ConfigMap missing Caddyfile key")
		}
	}
	w.lastResourceVersion = configMap.ResourceVersion
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
		watcher, err := w.clientset.CoreV1().ConfigMaps(w.namespace).Watch(ctx, metav1.ListOptions{
			FieldSelector:   "metadata.name=" + w.configMapName,
			ResourceVersion: w.lastResourceVersion,
		})
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
				logger.Error().Msg("Error in ConfigMap watch channel")
				break
			}
			cm, ok := event.Object.(*corev1.ConfigMap)
			if !ok {
				logger.Warn().Msg("Unexpected object in ConfigMap watch")
				continue
			}
			w.lastResourceVersion = cm.ResourceVersion
			if event.Type == watch.Added || event.Type == watch.Modified {
				if configData, exists := cm.Data["Caddyfile"]; exists {
					if w.nodeAvailableCheck() {
						logger.Info().Str("event", string(event.Type)).
							Msg("ConfigMap updated, notifying handlers")
						if !w.isPaused {
							w.configHandler(configData)
						}
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
