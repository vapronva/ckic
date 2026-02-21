package watcher

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/constants"
)

type ConfigHandlerFunc func(string)

type ConfigWatcher struct {
	mu                  sync.RWMutex
	clientset           *kubernetes.Clientset
	namespace           string
	configMapName       string
	configHandler       ConfigHandlerFunc
	forceSyncHandler    func()
	lastResourceVersion string
	nodeAvailableCheck  func() bool
	isPaused            bool
	cachedConfig        string
	hasCachedConfig     bool
	lastProcessedConfig string
	failureCount        int
	maxFailures         int
	resetTimeout        time.Duration
	lastSuccess         time.Time
}

const (
	configWatcherMaxFailures  = 5
	configWatcherResetTimeout = 5 * time.Minute
	configPauseSleep          = 10 * time.Second
)

func NewConfigWatcher(
	clientset *kubernetes.Clientset,
	namespace, configMapName string,
	handler ConfigHandlerFunc,
	nodeAvailableCheck func() bool,
) *ConfigWatcher {
	return &ConfigWatcher{
		clientset:          clientset,
		namespace:          namespace,
		configMapName:      configMapName,
		configHandler:      handler,
		nodeAvailableCheck: nodeAvailableCheck,
		isPaused:           false,
		maxFailures:        configWatcherMaxFailures,
		resetTimeout:       configWatcherResetTimeout,
		lastSuccess:        time.Now(),
	}
}

func (w *ConfigWatcher) Pause() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.isPaused {
		w.isPaused = true
		log.Info().Msg("ConfigMap watcher paused")
	}
}

func (w *ConfigWatcher) Resume() {
	w.mu.Lock()
	wasPaused := w.isPaused
	if wasPaused {
		w.isPaused = false
		log.Info().Msg("ConfigMap watcher resumed")
	}
	shouldProcess := w.hasCachedConfig && w.nodeAvailableCheck != nil && w.nodeAvailableCheck()
	var configToProcess string
	shouldSkip := false
	if shouldProcess {
		if w.cachedConfig != w.lastProcessedConfig {
			configToProcess = w.cachedConfig
		} else {
			shouldSkip = true
		}
		w.hasCachedConfig = false
	}
	w.mu.Unlock()
	if shouldSkip {
		log.Debug().Msg("Cached config unchanged, skipping handler on resume")
	} else if configToProcess != "" {
		w.configHandler(configToProcess)
		w.mu.Lock()
		w.lastProcessedConfig = configToProcess
		w.mu.Unlock()
	}
}

func (w *ConfigWatcher) SetForceSyncHandler(handler func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.forceSyncHandler = handler
}

func (w *ConfigWatcher) EnsureSync() {
	w.mu.Lock()
	if w.isPaused {
		w.isPaused = false
		log.Info().Msg("ConfigMap watcher resumed")
	}
	shouldProcessCached := w.hasCachedConfig && w.nodeAvailableCheck != nil &&
		w.nodeAvailableCheck()
	var cachedConfig string
	if shouldProcessCached {
		cachedConfig = w.cachedConfig
		w.hasCachedConfig = false
	}
	lastProcessed := w.lastProcessedConfig
	forceSyncHandler := w.forceSyncHandler
	configHandler := w.configHandler
	nodeAvailableCheck := w.nodeAvailableCheck
	w.mu.Unlock()
	if shouldProcessCached && cachedConfig != "" && configHandler != nil {
		configHandler(cachedConfig)
		w.mu.Lock()
		w.lastProcessedConfig = cachedConfig
		w.lastSuccess = time.Now()
		w.failureCount = 0
		w.mu.Unlock()
		if forceSyncHandler == nil {
			return
		}
	}
	if nodeAvailableCheck != nil && !nodeAvailableCheck() {
		return
	}
	if forceSyncHandler != nil {
		forceSyncHandler()
		return
	}
	if shouldProcessCached || lastProcessed == "" || configHandler == nil {
		return
	}
	configHandler(lastProcessed)
	w.mu.Lock()
	w.lastSuccess = time.Now()
	w.failureCount = 0
	w.mu.Unlock()
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (w *ConfigWatcher) refreshResourceVersion(ctx context.Context, logger zerolog.Logger) error {
	cm, err := w.clientset.CoreV1().
		ConfigMaps(w.namespace).
		Get(ctx, w.configMapName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	w.lastResourceVersion = cm.ResourceVersion
	logger.Info().Str("resourceVersion", w.lastResourceVersion).Msg("Resource version refreshed")
	if configData, exists := cm.Data["Caddyfile"]; exists {
		w.mu.RLock()
		lastProcessed := w.lastProcessedConfig
		w.mu.RUnlock()
		if configData == lastProcessed {
			logger.Debug().Msg("Refreshed ConfigMap content unchanged, skipping handler")
			return nil
		}
		if w.nodeAvailableCheck() {
			logger.Info().Msg("Refreshed ConfigMap loaded, updating configuration")
			w.configHandler(configData)
			w.mu.Lock()
			w.lastProcessedConfig = configData
			w.lastSuccess = time.Now()
			w.mu.Unlock()
		} else {
			logger.Info().Msg("Refreshed ConfigMap loaded but no eligible nodes, caching config")
			w.mu.Lock()
			w.cachedConfig = configData
			w.hasCachedConfig = true
			w.mu.Unlock()
		}
	}
	return nil
}

//nolint:gocognit,nestif,funlen
func (w *ConfigWatcher) Start(ctx context.Context) {
	logger := log.With().
		Str("component", "config_watcher").
		Str("namespace", w.namespace).
		Str("configmap", w.configMapName).
		Logger()
	logger.Info().Msg("Starting config watcher")
	configMap, err := w.clientset.CoreV1().
		ConfigMaps(w.namespace).
		Get(ctx, w.configMapName, metav1.GetOptions{})
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
		_, err = w.clientset.CoreV1().
			ConfigMaps(w.namespace).
			Create(ctx, configMap, metav1.CreateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create default ConfigMap")
		}
	} else {
		if configData, exists := configMap.Data["Caddyfile"]; exists {
			logger.Info().Msg("Initial ConfigMap loaded")
			if w.nodeAvailableCheck() {
				w.configHandler(configData)
				w.mu.Lock()
				w.lastProcessedConfig = configData
				w.mu.Unlock()
			} else {
				logger.Info().Msg("No eligible nodes available, caching initial config")
				w.mu.Lock()
				w.cachedConfig = configData
				w.hasCachedConfig = true
				w.mu.Unlock()
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
		w.mu.RLock()
		paused := w.isPaused
		w.mu.RUnlock()
		if paused {
			logger.Debug().Msg("Config watcher is paused; sleeping until resumed")
			time.Sleep(configPauseSleep)
			continue
		}
		watchOptions := metav1.ListOptions{
			FieldSelector:   "metadata.name=" + w.configMapName,
			ResourceVersion: "",
		}
		cmWatcher, watchErr := w.clientset.CoreV1().ConfigMaps(w.namespace).Watch(ctx, watchOptions)
		if watchErr != nil {
			logger.Error().Err(watchErr).Msg("Failed to create ConfigMap watcher, retrying")
			w.mu.Lock()
			w.failureCount++
			failureCount := w.failureCount
			lastSuccess := w.lastSuccess
			w.mu.Unlock()
			if failureCount >= w.maxFailures {
				sleepTime := w.resetTimeout - time.Since(lastSuccess)
				if sleepTime > 0 {
					logger.Warn().Msgf("Circuit breaker open, sleeping for %v", sleepTime)
					time.Sleep(sleepTime)
				}
				w.mu.Lock()
				w.failureCount = 0
				w.mu.Unlock()
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
		for event := range cmWatcher.ResultChan() {
			select {
			case <-ctx.Done():
				cmWatcher.Stop()
				logger.Info().Msg("Config watcher shutting down")
				return
			default:
			}
			if event.Type == watch.Error {
				if status, ok := event.Object.(*metav1.Status); ok && status.Code == 410 {
					logger.Info().
						Int32("code", status.Code).
						Msg("Received watch timeout (410); ignoring because we restart with an empty resourceVersion")
				} else if eventStatus, eventStatusOK := event.Object.(*metav1.Status); eventStatusOK {
					logger.Error().
						Int32("code", eventStatus.Code).
						Str("reason", string(eventStatus.Reason)).
						Str("message", eventStatus.Message).
						Msg("Error in ConfigMap watch channel")
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
					w.mu.RLock()
					lastProcessed := w.lastProcessedConfig
					w.mu.RUnlock()
					if configData == lastProcessed {
						logger.Debug().
							Str("event", string(event.Type)).
							Msg("ConfigMap content unchanged, skipping handler")
						continue
					}
					if w.nodeAvailableCheck() {
						logger.Info().
							Str("event", string(event.Type)).
							Msg("ConfigMap updated, notifying handlers")
						w.configHandler(configData)
						w.mu.Lock()
						w.lastProcessedConfig = configData
						w.lastSuccess = time.Now()
						w.failureCount = 0
						w.mu.Unlock()
					} else {
						logger.Info().
							Msg("ConfigMap updated but no eligible nodes available, caching config")
						w.mu.Lock()
						w.cachedConfig = configData
						w.hasCachedConfig = true
						w.mu.Unlock()
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
