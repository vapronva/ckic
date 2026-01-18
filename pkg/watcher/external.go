package watcher

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"gl.vprw.ru/vapronva/ckic/pkg/constants"
)

type ExternalConfigUpdateFunc func(namespace, fragment string)

type ExternalConfigRemoveFunc func(namespace string)

type ExternalConfigWatcher struct {
	clientset            *kubernetes.Clientset
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
}

func NewExternalConfigWatcher(
	clientset *kubernetes.Clientset,
	ownNamespace string,
	configMapName string,
	labelSelector string,
	nsMode string,
	allowNamespaces string,
	denyNamespaces string,
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
		maxFailures:          5,
		resetTimeout:         5 * time.Minute,
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
	logger.Info().Str("label", w.labelSelector).Str("mode", w.nsMode).Msg("Starting external config watcher")
	w.initialList(ctx, logger)
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
		watchOptions := metav1.ListOptions{
			LabelSelector:   w.labelSelector,
			ResourceVersion: "",
		}
		watcher, err := w.clientset.CoreV1().ConfigMaps("").Watch(ctx, watchOptions)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create external ConfigMap watcher, retrying")
			w.failureCount++
			if w.failureCount >= w.maxFailures {
				sleepTime := w.resetTimeout - time.Since(w.lastSuccess)
				if sleepTime > 0 {
					logger.Warn().Msgf("Circuit breaker open, sleeping for %v", sleepTime)
					time.Sleep(sleepTime)
				}
				w.failureCount = 0
				w.lastSuccess = time.Now()
			} else {
				time.Sleep(delay)
				delay = minExternalDuration(time.Duration(float64(delay)*multiplier), maxDelay)
			}
			continue
		}
		delay = constants.ConfigMapWatcherInitialDelay
		for event := range watcher.ResultChan() {
			select {
			case <-ctx.Done():
				watcher.Stop()
				logger.Info().Msg("External config watcher shutting down")
				return
			default:
			}
			if event.Type == watch.Error {
				if status, ok := event.Object.(*metav1.Status); ok && status.Code == 410 {
					logger.Info().Int32("code", status.Code).Msg("Received watch timeout (410); ignoring because we restart with an empty resourceVersion")
				} else {
					logger.Error().Msg("Error in external ConfigMap watch channel")
				}
				break
			}
			cm, ok := event.Object.(*corev1.ConfigMap)
			if !ok {
				logger.Warn().Msg("Unexpected object in external ConfigMap watch")
				continue
			}
			if !w.isNamespaceAllowed(cm.Namespace) {
				logger.Debug().Str("namespace", cm.Namespace).Msg("Skipping ConfigMap from excluded namespace")
				continue
			}
			w.lastResourceVersion = cm.ResourceVersion
			switch event.Type {
			case watch.Added, watch.Modified:
				if fragment, exists := cm.Data["Caddyfile"]; exists {
					sourceKey := fmt.Sprintf("%s/%s", cm.Namespace, cm.Name)
					if w.lastProcessedConfigs[sourceKey] == fragment {
						logger.Debug().Str("event", string(event.Type)).Str("namespace", cm.Namespace).Str("name", cm.Name).Msg("External ConfigMap content unchanged, skipping")
						continue
					}
					logger.Info().
						Str("event", string(event.Type)).
						Str("namespace", cm.Namespace).
						Str("name", cm.Name).
						Msg("External ConfigMap updated")
					if w.onUpdate != nil {
						w.onUpdate(sourceKey, fragment)
					}
					w.lastProcessedConfigs[sourceKey] = fragment
					w.lastSuccess = time.Now()
					w.failureCount = 0
				} else {
					logger.Warn().Str("namespace", cm.Namespace).Str("name", cm.Name).Msg("External ConfigMap missing 'Caddyfile' key")
				}
			case watch.Deleted:
				sourceKey := fmt.Sprintf("%s/%s", cm.Namespace, cm.Name)
				logger.Info().Str("namespace", cm.Namespace).Str("name", cm.Name).Msg("External ConfigMap deleted")
				if w.onRemove != nil {
					w.onRemove(sourceKey)
				}
				delete(w.lastProcessedConfigs, sourceKey)
				w.lastSuccess = time.Now()
				w.failureCount = 0
			}
		}
		logger.Info().Msg("External ConfigMap watch channel closed, restarting")
		time.Sleep(delay)
		delay = minExternalDuration(time.Duration(float64(delay)*multiplier), maxDelay)
	}
}

func (w *ExternalConfigWatcher) initialList(ctx context.Context, logger zerolog.Logger) {
	listOptions := metav1.ListOptions{
		LabelSelector: w.labelSelector,
	}
	configMaps, err := w.clientset.CoreV1().ConfigMaps("").List(ctx, listOptions)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to list initial external ConfigMaps")
		return
	}
	w.lastResourceVersion = configMaps.ResourceVersion
	logger.Info().Int("count", len(configMaps.Items)).Str("resourceVersion", w.lastResourceVersion).Msg("Discovered initial external ConfigMaps")
	for _, cm := range configMaps.Items {
		if !w.isNamespaceAllowed(cm.Namespace) {
			continue
		}
		if fragment, exists := cm.Data["Caddyfile"]; exists {
			sourceKey := fmt.Sprintf("%s/%s", cm.Namespace, cm.Name)
			logger.Info().Str("namespace", cm.Namespace).Str("name", cm.Name).Msg("Loading initial external ConfigMap")
			if w.onUpdate != nil {
				w.onUpdate(sourceKey, fragment)
			}
			w.lastProcessedConfigs[sourceKey] = fragment
		}
	}
	w.lastSuccess = time.Now()
}

func minExternalDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
