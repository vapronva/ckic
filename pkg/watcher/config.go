package watcher

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/constants"
	"git.horse/vapronva/ckic/pkg/utils"
)

type ConfigHandlerFunc func(string)

type ConfigWatcher struct {
	mu                  sync.RWMutex
	clientset           kubernetes.Interface
	namespace           string
	configMapName       string
	configHandler       ConfigHandlerFunc
	forceSyncHandler    func()
	nodeAvailableCheck  func() bool
	isPaused            bool
	cachedConfig        string
	hasCachedConfig     bool
	lastProcessedConfig string
	configSeq           uint64
	failureCount        int
	maxFailures         int
	resetTimeout        time.Duration
	lastSuccess         time.Time
	bootstrapDefault    bool
}

const (
	configWatcherMaxFailures  = 5
	configWatcherResetTimeout = 5 * time.Minute
	configPauseSleep          = 10 * time.Second
)

func NewConfigWatcher(
	clientset kubernetes.Interface,
	namespace, configMapName string,
	handler ConfigHandlerFunc,
	nodeAvailableCheck func() bool,
	bootstrapDefault bool,
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
		bootstrapDefault:   bootstrapDefault,
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

func (w *ConfigWatcher) recordSuccessAndDispatch(configData string) {
	w.mu.Lock()
	w.lastProcessedConfig = configData
	w.configSeq++
	w.lastSuccess = time.Now()
	w.failureCount = 0
	w.mu.Unlock()
	w.configHandler(configData)
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
	startSeq := w.configSeq
	forceSyncHandler := w.forceSyncHandler
	configHandler := w.configHandler
	nodeAvailableCheck := w.nodeAvailableCheck
	w.mu.Unlock()
	if shouldProcessCached && cachedConfig != "" && configHandler != nil {
		w.mu.Lock()
		w.lastProcessedConfig = cachedConfig
		w.configSeq++
		w.lastSuccess = time.Now()
		w.failureCount = 0
		w.mu.Unlock()
		configHandler(cachedConfig)
		if forceSyncHandler == nil {
			return
		}
	}
	if nodeAvailableCheck != nil && !nodeAvailableCheck() {
		return
	}
	if forceSyncHandler != nil {
		forceSyncHandler()
		w.mu.Lock()
		w.lastSuccess = time.Now()
		w.failureCount = 0
		w.mu.Unlock()
		return
	}
	if shouldProcessCached || lastProcessed == "" || configHandler == nil {
		return
	}
	w.mu.Lock()
	if w.configSeq != startSeq {
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()
	configHandler(lastProcessed)
	w.mu.Lock()
	w.lastSuccess = time.Now()
	w.failureCount = 0
	w.mu.Unlock()
}

func (w *ConfigWatcher) reloadConfig(
	ctx context.Context,
	logger zerolog.Logger,
) error {
	cm, err := w.clientset.CoreV1().
		ConfigMaps(w.namespace).
		Get(ctx, w.configMapName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	logger.Info().Msg("Reloaded ConfigMap after circuit-breaker cooldown")
	if configData, exists := cm.Data[constants.CaddyfileKey]; exists {
		w.mu.RLock()
		lastProcessed := w.lastProcessedConfig
		w.mu.RUnlock()
		if configData == lastProcessed {
			logger.Debug().Msg("Refreshed ConfigMap content unchanged, skipping handler")
			return nil
		}
		if w.nodeAvailableCheck() {
			logger.Info().Msg("Refreshed ConfigMap loaded, updating configuration")
			w.recordSuccessAndDispatch(configData)
		} else {
			logger.Info().
				Msg("Refreshed ConfigMap loaded but no eligible nodes, caching config")
			w.mu.Lock()
			w.cachedConfig = configData
			w.hasCachedConfig = true
			w.mu.Unlock()
		}
	}
	return nil
}

func (w *ConfigWatcher) Start(ctx context.Context) {
	logger := log.With().
		Str("component", "config_watcher").
		Str("namespace", w.namespace).
		Str("configmap", w.configMapName).
		Logger()
	logger.Info().Msg("Starting config watcher")
	w.loadInitialConfig(ctx, logger)
	delay := constants.ConfigMapWatcherInitialDelay
	maxDelay := constants.ConfigMapWatcherMaxDelay
	multiplier := 1.5
	for {
		if w.shouldStop(ctx, logger) {
			return
		}
		if w.isWatcherPaused() {
			logger.Debug().Msg("Config watcher is paused; sleeping until resumed")
			if !utils.SleepCtx(ctx, configPauseSleep) {
				return
			}
			continue
		}
		nextDelay, keepRunning := w.runConfigWatchCycle(
			ctx,
			logger,
			delay,
			maxDelay,
			multiplier,
		)
		if !keepRunning {
			return
		}
		delay = nextDelay
	}
}

func (w *ConfigWatcher) loadInitialConfig(ctx context.Context, logger zerolog.Logger) {
	configMap := w.getInitialConfigMap(ctx, logger)
	if configMap == nil {
		return
	}
	w.applyInitialConfig(configMap, logger)
}

func (w *ConfigWatcher) shouldStop(ctx context.Context, logger zerolog.Logger) bool {
	select {
	case <-ctx.Done():
		logger.Info().Msg("Config watcher shutting down")
		return true
	default:
		return false
	}
}

func (w *ConfigWatcher) isWatcherPaused() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.isPaused
}

func (w *ConfigWatcher) runConfigWatchCycle(
	ctx context.Context,
	logger zerolog.Logger,
	delay, maxDelay time.Duration,
	multiplier float64,
) (time.Duration, bool) {
	watchOptions := metav1.ListOptions{
		FieldSelector:   "metadata.name=" + w.configMapName,
		ResourceVersion: "",
	}
	cmWatcher, watchErr := w.clientset.CoreV1().
		ConfigMaps(w.namespace).
		Watch(ctx, watchOptions)
	if watchErr != nil {
		return w.handleWatchCreateFailure(
			ctx,
			logger,
			watchErr,
			delay,
			maxDelay,
			multiplier,
		), true
	}
	delay = constants.ConfigMapWatcherInitialDelay
	if !w.consumeConfigEvents(ctx, cmWatcher, logger) {
		return delay, false
	}
	logger.Info().Msg("ConfigMap watch channel closed, restarting")
	if !utils.SleepCtx(ctx, delay) {
		return delay, false
	}
	return min(time.Duration(float64(delay)*multiplier), maxDelay), true
}

func (w *ConfigWatcher) handleWatchCreateFailure(
	ctx context.Context,
	logger zerolog.Logger,
	watchErr error,
	delay, maxDelay time.Duration,
	multiplier float64,
) time.Duration {
	logger.Error().
		Err(watchErr).
		Msg("Failed to create ConfigMap watcher, retrying")
	w.mu.Lock()
	w.failureCount++
	failureCount := w.failureCount
	w.mu.Unlock()
	if failureCount >= w.maxFailures {
		w.mu.RLock()
		lastSuccess := w.lastSuccess
		w.mu.RUnlock()
		sleepTime := w.resetTimeout - time.Since(lastSuccess)
		if sleepTime > 0 {
			logger.Warn().Msgf("Circuit breaker open, sleeping for %v", sleepTime)
			if !utils.SleepCtx(ctx, sleepTime) {
				return delay
			}
		}
		w.mu.Lock()
		w.failureCount = 0
		w.lastSuccess = time.Now()
		w.mu.Unlock()
		if reloadErr := w.reloadConfig(ctx, logger); reloadErr != nil {
			logger.Error().Err(reloadErr).Msg("Failed to reload ConfigMap")
		}
		return delay
	}
	if !utils.SleepCtx(ctx, delay) {
		return delay
	}
	return min(time.Duration(float64(delay)*multiplier), maxDelay)
}

func (w *ConfigWatcher) consumeConfigEvents(
	ctx context.Context,
	cmWatcher watch.Interface,
	logger zerolog.Logger,
) bool {
	for event := range cmWatcher.ResultChan() {
		select {
		case <-ctx.Done():
			cmWatcher.Stop()
			logger.Info().Msg("Config watcher shutting down")
			return false
		default:
		}
		if w.handleConfigWatchEvent(event, logger) {
			return true
		}
	}
	return true
}

func (w *ConfigWatcher) handleConfigWatchEvent(
	event watch.Event,
	logger zerolog.Logger,
) bool {
	if event.Type == watch.Error {
		logConfigWatchError(event, logger)
		return true
	}
	cm, ok := event.Object.(*corev1.ConfigMap)
	if !ok {
		logger.Warn().Msg("Unexpected object in ConfigMap watch")
		return false
	}
	if event.Type == watch.Added || event.Type == watch.Modified {
		w.handleConfigMapUpdate(cm, event.Type, logger)
	}
	return false
}

func logConfigWatchError(event watch.Event, logger zerolog.Logger) {
	if status, ok := event.Object.(*metav1.Status); ok && status.Code == 410 {
		logger.Info().
			Int32("code", status.Code).
			Msg("Received watch timeout (410); ignoring because we restart with an empty resourceVersion")
		return
	}
	if eventStatus, ok := event.Object.(*metav1.Status); ok {
		logger.Error().
			Int32("code", eventStatus.Code).
			Str("reason", string(eventStatus.Reason)).
			Str("message", eventStatus.Message).
			Msg("Error in ConfigMap watch channel")
		return
	}
	logger.Error().Msg("Error in ConfigMap watch channel")
}

func (w *ConfigWatcher) handleConfigMapUpdate(
	cm *corev1.ConfigMap,
	eventType watch.EventType,
	logger zerolog.Logger,
) {
	configData, exists := cm.Data[constants.CaddyfileKey]
	if !exists {
		logger.Warn().Msg("Updated ConfigMap missing Caddyfile key")
		return
	}
	w.mu.RLock()
	lastProcessed := w.lastProcessedConfig
	w.mu.RUnlock()
	if configData == lastProcessed {
		logger.Debug().
			Str("event", string(eventType)).
			Msg("ConfigMap content unchanged, skipping handler")
		return
	}
	if w.nodeAvailableCheck() {
		logger.Info().
			Str("event", string(eventType)).
			Msg("ConfigMap updated, notifying handlers")
		w.recordSuccessAndDispatch(configData)
		return
	}
	logger.Info().Msg("ConfigMap updated but no eligible nodes available, caching config")
	w.mu.Lock()
	w.cachedConfig = configData
	w.hasCachedConfig = true
	w.mu.Unlock()
	w.Pause()
}

func (w *ConfigWatcher) getInitialConfigMap(
	ctx context.Context,
	logger zerolog.Logger,
) *corev1.ConfigMap {
	configMap, err := w.clientset.CoreV1().
		ConfigMaps(w.namespace).
		Get(ctx, w.configMapName, metav1.GetOptions{})
	if err == nil {
		return configMap
	}
	if !apierrors.IsNotFound(err) {
		logger.Error().Err(err).Msg("Failed to get initial ConfigMap")
		return nil
	}
	if !w.bootstrapDefault {
		logger.Warn().
			Msg("Initial ConfigMap not found and bootstrap is disabled, waiting for ConfigMap to be created")
		return nil
	}
	logger.Warn().
		Msg("Initial ConfigMap not found and bootstrap is enabled, creating default ConfigMap")
	return w.createDefaultInitialConfigMap(ctx, logger)
}

func (w *ConfigWatcher) createDefaultInitialConfigMap(
	ctx context.Context,
	logger zerolog.Logger,
) *corev1.ConfigMap {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      w.configMapName,
			Namespace: w.namespace,
		},
		Data: map[string]string{
			constants.CaddyfileKey: ":80 {\n    respond \"Hello, world!\"\n}\n",
		},
	}
	_, err := w.clientset.CoreV1().
		ConfigMaps(w.namespace).
		Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create default ConfigMap")
		return nil
	}
	return configMap
}

func (w *ConfigWatcher) applyInitialConfig(
	configMap *corev1.ConfigMap,
	logger zerolog.Logger,
) {
	configData, exists := configMap.Data[constants.CaddyfileKey]
	if !exists {
		logger.Warn().Msg("ConfigMap missing Caddyfile key")
		return
	}
	logger.Info().Msg("Initial ConfigMap loaded")
	if w.nodeAvailableCheck() {
		w.mu.Lock()
		w.lastProcessedConfig = configData
		w.configSeq++
		w.mu.Unlock()
		w.configHandler(configData)
		return
	}
	logger.Info().Msg("No eligible nodes available, caching initial config")
	w.mu.Lock()
	w.cachedConfig = configData
	w.hasCachedConfig = true
	w.mu.Unlock()
}
