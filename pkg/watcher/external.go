package watcher

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/constants"
)

type ExternalConfigUpdateFunc func(namespace, fragment string)

type ExternalConfigRemoveFunc func(namespace string)

type ExternalConfigWatcher struct {
	clientset            kubernetes.Interface
	ownNamespace         string
	configMapName        string
	labelSelector        string
	nsMode               string
	allowedNamespaces    map[string]bool
	deniedNamespaces     map[string]bool
	onUpdate             ExternalConfigUpdateFunc
	onRemove             ExternalConfigRemoveFunc
	lastResourceVersion  string
	lastProcessedConfigs map[string]string
	failureCount         int
	maxFailures          int
	resetTimeout         time.Duration
	lastSuccess          time.Time
	batchInitialized     bool
}

const (
	externalWatcherMaxFailures  = 5
	externalWatcherResetTimeout = 5 * time.Minute
)

func NewExternalConfigWatcher(
	clientset kubernetes.Interface,
	ownNamespace, configMapName, labelSelector, nsMode, allowNamespaces, denyNamespaces string,
	onUpdate ExternalConfigUpdateFunc,
	onRemove ExternalConfigRemoveFunc,
) *ExternalConfigWatcher {
	allowedNs := make(map[string]bool)
	deniedNs := make(map[string]bool)
	if allowNamespaces != "" {
		for ns := range strings.SplitSeq(allowNamespaces, ",") {
			ns = strings.TrimSpace(ns)
			if ns != "" {
				allowedNs[ns] = true
			}
		}
	}
	if denyNamespaces != "" {
		for ns := range strings.SplitSeq(denyNamespaces, ",") {
			ns = strings.TrimSpace(ns)
			if ns != "" {
				deniedNs[ns] = true
			}
		}
	}
	return &ExternalConfigWatcher{
		clientset:            clientset,
		ownNamespace:         ownNamespace,
		configMapName:        configMapName,
		labelSelector:        labelSelector,
		nsMode:               nsMode,
		allowedNamespaces:    allowedNs,
		deniedNamespaces:     deniedNs,
		onUpdate:             onUpdate,
		onRemove:             onRemove,
		lastProcessedConfigs: make(map[string]string),
		maxFailures:          externalWatcherMaxFailures,
		resetTimeout:         externalWatcherResetTimeout,
		lastSuccess:          time.Now(),
	}
}

func (w *ExternalConfigWatcher) isNamespaceAllowed(namespace string) bool {
	if namespace == w.ownNamespace {
		return false
	}
	switch w.nsMode {
	case "all":
		return true
	case "allow":
		return w.allowedNamespaces[namespace]
	case "deny":
		return !w.deniedNamespaces[namespace]
	default:
		return false
	}
}

func (w *ExternalConfigWatcher) Start(ctx context.Context) {
	logger := log.With().Str("component", "external_config_watcher").Logger()
	logger.Info().
		Str("label", w.labelSelector).
		Str("mode", w.nsMode).
		Msg("Starting external config watcher")
	if !w.batchInitialized {
		w.initialList(ctx, logger)
	} else {
		logger.Info().Msg("Skipping initial list, batch initialization already performed")
	}
	delay := constants.ConfigMapWatcherInitialDelay
	maxDelay := constants.ConfigMapWatcherMaxDelay
	multiplier := 1.5
	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("External config watcher shutting down")
			return
		default:
		}
		newDelay, keepRunning := w.runWatchCycle(
			ctx,
			logger,
			delay,
			maxDelay,
			multiplier,
		)
		if !keepRunning {
			return
		}
		delay = newDelay
	}
}

func (w *ExternalConfigWatcher) runWatchCycle(
	ctx context.Context,
	logger zerolog.Logger,
	delay, maxDelay time.Duration,
	multiplier float64,
) (time.Duration, bool) {
	watchOptions := metav1.ListOptions{
		LabelSelector:   w.labelSelector,
		ResourceVersion: w.lastResourceVersion,
	}
	watcher, err := w.clientset.CoreV1().ConfigMaps("").Watch(ctx, watchOptions)
	if err != nil {
		nextDelay := w.handleWatchCreateError(
			ctx,
			logger,
			err,
			delay,
			maxDelay,
			multiplier,
		)
		return nextDelay, true
	}
	delay = constants.ConfigMapWatcherInitialDelay
	if !w.consumeExternalEvents(ctx, watcher, logger) {
		return delay, false
	}
	logger.Info().Msg("External ConfigMap watch channel closed, restarting")
	time.Sleep(delay)
	return min(time.Duration(float64(delay)*multiplier), maxDelay), true
}

func (w *ExternalConfigWatcher) consumeExternalEvents(
	ctx context.Context,
	watcher watch.Interface,
	logger zerolog.Logger,
) bool {
	for event := range watcher.ResultChan() {
		select {
		case <-ctx.Done():
			watcher.Stop()
			logger.Info().Msg("External config watcher shutting down")
			return false
		default:
		}
		if w.handleExternalEvent(ctx, event, logger) {
			return true
		}
	}
	return true
}

func (w *ExternalConfigWatcher) handleExternalEvent(
	ctx context.Context,
	event watch.Event,
	logger zerolog.Logger,
) bool {
	if event.Type == watch.Error {
		w.handleWatchErrorEvent(ctx, logger, event)
		return true
	}
	cm, ok := event.Object.(*corev1.ConfigMap)
	if !ok {
		logger.Warn().Msg("Unexpected object in external ConfigMap watch")
		return false
	}
	w.lastResourceVersion = cm.ResourceVersion
	if !w.isNamespaceAllowed(cm.Namespace) {
		logger.Debug().
			Str("namespace", cm.Namespace).
			Msg("Skipping ConfigMap from excluded namespace")
		return false
	}
	switch event.Type {
	case watch.Added, watch.Modified:
		w.handleWatchUpsert(cm, event.Type, logger)
	case watch.Deleted:
		w.handleWatchDelete(cm, logger)
	case watch.Bookmark:
		logger.Debug().Msg("Received external ConfigMap bookmark event")
	case watch.Error:
	}
	return false
}

func (w *ExternalConfigWatcher) handleWatchUpsert(
	cm *corev1.ConfigMap,
	eventType watch.EventType,
	logger zerolog.Logger,
) {
	sourceKey := configMapSourceKey(cm.Namespace, cm.Name)
	fragment, exists := cm.Data[constants.CaddyfileKey]
	if !exists {
		logger.Warn().
			Str("namespace", cm.Namespace).
			Str("name", cm.Name).
			Msg("External ConfigMap missing 'Caddyfile' key")
		if _, processed := w.lastProcessedConfigs[sourceKey]; !processed {
			return
		}
		logger.Info().
			Str("namespace", cm.Namespace).
			Str("name", cm.Name).
			Msg("Removing stale external fragment after missing 'Caddyfile' key")
		w.removeExternalSource(sourceKey)
		w.markWatchSuccess()
		return
	}
	if w.lastProcessedConfigs[sourceKey] == fragment {
		logger.Debug().
			Str("event", string(eventType)).
			Str("namespace", cm.Namespace).
			Str("name", cm.Name).
			Msg("External ConfigMap content unchanged, skipping")
		return
	}
	logger.Info().
		Str("event", string(eventType)).
		Str("namespace", cm.Namespace).
		Str("name", cm.Name).
		Msg("External ConfigMap updated")
	if w.onUpdate != nil {
		w.onUpdate(sourceKey, fragment)
	}
	w.lastProcessedConfigs[sourceKey] = fragment
	w.markWatchSuccess()
}

func (w *ExternalConfigWatcher) handleWatchDelete(
	cm *corev1.ConfigMap,
	logger zerolog.Logger,
) {
	sourceKey := configMapSourceKey(cm.Namespace, cm.Name)
	logger.Info().
		Str("namespace", cm.Namespace).
		Str("name", cm.Name).
		Msg("External ConfigMap deleted")
	w.removeExternalSource(sourceKey)
	w.markWatchSuccess()
}

func (w *ExternalConfigWatcher) removeExternalSource(sourceKey string) {
	if w.onRemove != nil {
		w.onRemove(sourceKey)
	}
	delete(w.lastProcessedConfigs, sourceKey)
}

func (w *ExternalConfigWatcher) markWatchSuccess() {
	w.lastSuccess = time.Now()
	w.failureCount = 0
}

func (w *ExternalConfigWatcher) handleWatchCreateError(
	ctx context.Context,
	logger zerolog.Logger,
	err error,
	delay, maxDelay time.Duration,
	multiplier float64,
) time.Duration {
	if isExternalResourceVersionExpired(err) {
		logger.Warn().
			Err(err).
			Str("resourceVersion", w.lastResourceVersion).
			Msg("External watch resourceVersion expired, reconciling from full list")
		reconcileErr := w.reconcileFromList(ctx, logger)
		if reconcileErr == nil {
			return constants.ConfigMapWatcherInitialDelay
		}
		logger.Error().
			Err(reconcileErr).
			Msg("Failed to reconcile external ConfigMaps after expired watch")
	}
	logger.Error().Err(err).Msg("Failed to create external ConfigMap watcher, retrying")
	w.failureCount++
	if w.failureCount < w.maxFailures {
		time.Sleep(delay)
		return min(time.Duration(float64(delay)*multiplier), maxDelay)
	}
	sleepTime := w.resetTimeout - time.Since(w.lastSuccess)
	if sleepTime > 0 {
		logger.Warn().Msgf("Circuit breaker open, sleeping for %v", sleepTime)
		time.Sleep(sleepTime)
	}
	w.failureCount = 0
	return delay
}

func (w *ExternalConfigWatcher) handleWatchErrorEvent(
	ctx context.Context,
	logger zerolog.Logger,
	event watch.Event,
) {
	status, statusOK := event.Object.(*metav1.Status)
	if !statusOK {
		logger.Error().Msg("Error in external ConfigMap watch channel")
		return
	}
	if !isExternalWatchExpiredStatus(status) {
		logger.Error().
			Int32("code", status.Code).
			Str("reason", string(status.Reason)).
			Str("message", status.Message).
			Msg("Error in external ConfigMap watch channel")
		return
	}
	logger.Warn().
		Int32("code", status.Code).
		Str("resourceVersion", w.lastResourceVersion).
		Msg("Received expired external watch event, reconciling from full list")
	if reconcileErr := w.reconcileFromList(ctx, logger); reconcileErr != nil {
		logger.Error().
			Err(reconcileErr).
			Msg("Failed to reconcile external ConfigMaps after expired watch event")
	}
}

func (w *ExternalConfigWatcher) listFilteredFragments(
	ctx context.Context,
) (*corev1.ConfigMapList, map[string]string, error) {
	configMaps, err := w.clientset.CoreV1().ConfigMaps("").List(ctx, metav1.ListOptions{
		LabelSelector: w.labelSelector,
	})
	if err != nil {
		return nil, nil, err
	}
	fragments := make(map[string]string)
	for _, cm := range configMaps.Items {
		if !w.isNamespaceAllowed(cm.Namespace) {
			continue
		}
		if fragment, exists := cm.Data[constants.CaddyfileKey]; exists {
			fragments[configMapSourceKey(cm.Namespace, cm.Name)] = fragment
		}
	}
	return configMaps, fragments, nil
}

func (w *ExternalConfigWatcher) initialList(ctx context.Context, logger zerolog.Logger) {
	configMaps, fragments, err := w.listFilteredFragments(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to list initial external ConfigMaps")
		return
	}
	w.lastResourceVersion = configMaps.ResourceVersion
	logger.Info().
		Int("count", len(fragments)).
		Str("resourceVersion", w.lastResourceVersion).
		Msg("Discovered initial external ConfigMaps")
	for sourceKey, fragment := range fragments {
		logger.Info().Str("source", sourceKey).Msg("Loading initial external ConfigMap")
		if w.onUpdate != nil {
			w.onUpdate(sourceKey, fragment)
		}
		w.lastProcessedConfigs[sourceKey] = fragment
	}
	w.lastSuccess = time.Now()
}

func (w *ExternalConfigWatcher) reconcileFromList(
	ctx context.Context,
	logger zerolog.Logger,
) error {
	listOptions := metav1.ListOptions{
		LabelSelector: w.labelSelector,
	}
	configMaps, err := w.clientset.CoreV1().ConfigMaps("").List(ctx, listOptions)
	if err != nil {
		return fmt.Errorf(
			"failed to list external ConfigMaps for reconciliation: %w",
			err,
		)
	}
	w.reconcileSnapshot(configMaps, logger)
	return nil
}

func (w *ExternalConfigWatcher) reconcileSnapshot(
	configMaps *corev1.ConfigMapList,
	logger zerolog.Logger,
) {
	seen := make(map[string]struct{}, len(configMaps.Items))
	updates := 0
	removals := 0
	for _, cm := range configMaps.Items {
		deltaUpdates, deltaRemovals := w.reconcileSnapshotConfigMap(cm, seen, logger)
		updates += deltaUpdates
		removals += deltaRemovals
	}
	removals += w.reconcileSnapshotStaleEntries(seen, logger)
	w.lastResourceVersion = configMaps.ResourceVersion
	w.lastSuccess = time.Now()
	w.failureCount = 0
	logger.Info().
		Int("count", len(configMaps.Items)).
		Int("updates", updates).
		Int("removals", removals).
		Str("resourceVersion", w.lastResourceVersion).
		Msg("External ConfigMap reconciliation complete")
}

func (w *ExternalConfigWatcher) reconcileSnapshotConfigMap(
	cm corev1.ConfigMap,
	seen map[string]struct{},
	logger zerolog.Logger,
) (int, int) {
	if !w.isNamespaceAllowed(cm.Namespace) {
		return 0, 0
	}
	sourceKey := configMapSourceKey(cm.Namespace, cm.Name)
	fragment, exists := cm.Data[constants.CaddyfileKey]
	if !exists {
		logger.Warn().
			Str("namespace", cm.Namespace).
			Str("name", cm.Name).
			Msg("External ConfigMap missing 'Caddyfile' key during reconciliation")
		if _, processed := w.lastProcessedConfigs[sourceKey]; !processed {
			return 0, 0
		}
		logger.Info().
			Str("namespace", cm.Namespace).
			Str("name", cm.Name).
			Msg("Reconciling stale external fragment removal after missing 'Caddyfile' key")
		w.removeExternalSource(sourceKey)
		return 0, 1
	}
	seen[sourceKey] = struct{}{}
	if w.lastProcessedConfigs[sourceKey] == fragment {
		return 0, 0
	}
	logger.Info().
		Str("namespace", cm.Namespace).
		Str("name", cm.Name).
		Msg("Reconciling external ConfigMap update")
	if w.onUpdate != nil {
		w.onUpdate(sourceKey, fragment)
	}
	w.lastProcessedConfigs[sourceKey] = fragment
	return 1, 0
}

func (w *ExternalConfigWatcher) reconcileSnapshotStaleEntries(
	seen map[string]struct{},
	logger zerolog.Logger,
) int {
	removals := 0
	for sourceKey := range w.lastProcessedConfigs {
		if _, exists := seen[sourceKey]; exists {
			continue
		}
		logger.Info().
			Str("source", sourceKey).
			Msg("Reconciling stale external ConfigMap removal")
		w.removeExternalSource(sourceKey)
		removals++
	}
	return removals
}

func (w *ExternalConfigWatcher) InitialListBatch(
	ctx context.Context,
) (map[string]string, error) {
	logger := log.With().Str("component", "external_config_watcher").Logger()
	configMaps, fragments, err := w.listFilteredFragments(ctx)
	if err != nil {
		logger.Error().
			Err(err).
			Msg("Failed to list external ConfigMaps for batch initialization")
		return nil, fmt.Errorf("failed to list external ConfigMaps: %w", err)
	}
	w.lastResourceVersion = configMaps.ResourceVersion
	maps.Copy(w.lastProcessedConfigs, fragments)
	w.batchInitialized = true
	w.lastSuccess = time.Now()
	logger.Info().
		Int("count", len(fragments)).
		Str("resourceVersion", w.lastResourceVersion).
		Msg("Batch loaded external ConfigMaps")
	return fragments, nil
}

func configMapSourceKey(namespace, name string) string {
	return namespace + "/" + name
}

func isExternalResourceVersionExpired(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
		return true
	}
	statusErr := &apierrors.StatusError{}
	ok := errors.As(err, &statusErr)
	if !ok {
		return false
	}
	return isExternalWatchExpiredStatus(&statusErr.ErrStatus)
}

func isExternalWatchExpiredStatus(status *metav1.Status) bool {
	if status == nil {
		return false
	}
	return status.Code == 410 || status.Reason == metav1.StatusReasonExpired
}
