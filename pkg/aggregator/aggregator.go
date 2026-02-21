package aggregator

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type NamespaceAggregator struct {
	mu                      sync.RWMutex
	publishMu               sync.Mutex
	nodePushMu              sync.Mutex
	stateVersion            uint64
	lastPushedVersion       uint64
	lastPublishedVersion    uint64
	base                    string
	externals               map[string]string
	lastPushedMerged        string
	lastPublishedToMirror   string
	clientset               *kubernetes.Clientset
	namespace               string
	publishAggregated       bool
	aggregatedConfigMapName string
	configUpdateHandler     func(string)
	nodeAvailabilityCheck   func() bool
	initializing            bool
}

func NewNamespaceAggregator(
	clientset *kubernetes.Clientset,
	namespace string,
	publishAggregated bool,
	aggregatedConfigMapName string,
	configUpdateHandler func(string),
	nodeAvailabilityCheck func() bool,
) *NamespaceAggregator {
	return &NamespaceAggregator{
		externals:               make(map[string]string),
		clientset:               clientset,
		namespace:               namespace,
		publishAggregated:       publishAggregated,
		aggregatedConfigMapName: aggregatedConfigMapName,
		configUpdateHandler:     configUpdateHandler,
		nodeAvailabilityCheck:   nodeAvailabilityCheck,
		initializing:            true,
	}
}

func (a *NamespaceAggregator) UpdateBase(base string) {
	logger := log.With().Str("component", "aggregator").Logger()
	a.mu.Lock()
	changed := a.base != base
	a.base = base
	if changed {
		a.stateVersion++
	}
	version := a.stateVersion
	initializing := a.initializing
	merged := a.currentMergedLocked()
	mirrorUnchanged := version <= a.lastPublishedVersion
	nodeUnchanged := version <= a.lastPushedVersion
	a.mu.Unlock()
	if initializing {
		logger.Debug().Msg("Base updated during initialization, deferring push")
		return
	}
	if a.publishAggregated && !mirrorUnchanged {
		a.publishMu.Lock()
		a.mu.Lock()
		stalePublish := version < a.stateVersion || version <= a.lastPublishedVersion
		a.mu.Unlock()
		if stalePublish {
			logger.Debug().Msg("Skipping stale mirror publish for base update")
		} else {
			if err := a.publishMirrorConfigMap(merged); err != nil {
				logger.Error().Err(err).Msg("Failed to publish aggregated ConfigMap")
			} else {
				a.mu.Lock()
				if version > a.lastPublishedVersion {
					a.lastPublishedToMirror = merged
					a.lastPublishedVersion = version
				}
				a.mu.Unlock()
			}
		}
		a.publishMu.Unlock()
	}
	if nodeUnchanged {
		logger.Debug().Msg("Base updated but merged config unchanged for nodes, skipping push")
		return
	}
	if a.nodeAvailabilityCheck != nil && !a.nodeAvailabilityCheck() {
		logger.Info().Msg("Base updated but no nodes available, skipping config push")
		return
	}
	if a.configUpdateHandler != nil {
		a.nodePushMu.Lock()
		a.mu.Lock()
		stalePush := version < a.stateVersion || version <= a.lastPushedVersion
		a.mu.Unlock()
		if stalePush {
			logger.Debug().Msg("Skipping stale node push for base update")
		} else {
			a.configUpdateHandler(merged)
			a.mu.Lock()
			if version > a.lastPushedVersion {
				a.lastPushedMerged = merged
				a.lastPushedVersion = version
			}
			a.mu.Unlock()
		}
		a.nodePushMu.Unlock()
	}
}

func (a *NamespaceAggregator) SetExternal(namespace, fragment string) {
	logger := log.With().Str("component", "aggregator").Str("namespace", namespace).Logger()
	a.mu.Lock()
	changed := a.externals[namespace] != fragment
	a.externals[namespace] = fragment
	if changed {
		a.stateVersion++
	}
	version := a.stateVersion
	initializing := a.initializing
	merged := a.currentMergedLocked()
	mirrorUnchanged := version <= a.lastPublishedVersion
	nodeUnchanged := version <= a.lastPushedVersion
	a.mu.Unlock()
	if initializing {
		logger.Debug().Msg("External fragment updated during initialization, deferring push")
		return
	}
	if a.publishAggregated && !mirrorUnchanged {
		logger.Info().Msg("External fragment updated, publishing to mirror")
		a.publishMu.Lock()
		a.mu.Lock()
		stalePublish := version < a.stateVersion || version <= a.lastPublishedVersion
		a.mu.Unlock()
		if stalePublish {
			logger.Debug().Msg("Skipping stale mirror publish for external update")
		} else {
			if err := a.publishMirrorConfigMap(merged); err != nil {
				logger.Error().Err(err).Msg("Failed to publish aggregated ConfigMap")
			} else {
				a.mu.Lock()
				if version > a.lastPublishedVersion {
					a.lastPublishedToMirror = merged
					a.lastPublishedVersion = version
				}
				a.mu.Unlock()
			}
		}
		a.publishMu.Unlock()
	}
	if nodeUnchanged {
		logger.Debug().Msg("External fragment updated but merged config unchanged for nodes, skipping push")
		return
	}
	logger.Info().Msg("External fragment updated, pushing to nodes")
	if a.nodeAvailabilityCheck != nil && !a.nodeAvailabilityCheck() {
		logger.Info().Msg("External updated but no nodes available, skipping config push")
		return
	}
	if a.configUpdateHandler != nil {
		a.nodePushMu.Lock()
		a.mu.Lock()
		stalePush := version < a.stateVersion || version <= a.lastPushedVersion
		a.mu.Unlock()
		if stalePush {
			logger.Debug().Msg("Skipping stale node push for external update")
		} else {
			a.configUpdateHandler(merged)
			a.mu.Lock()
			if version > a.lastPushedVersion {
				a.lastPushedMerged = merged
				a.lastPushedVersion = version
			}
			a.mu.Unlock()
		}
		a.nodePushMu.Unlock()
	}
}

func (a *NamespaceAggregator) SetExternalBatch(externals map[string]string) {
	logger := log.With().Str("component", "aggregator").Logger()
	a.mu.Lock()
	changed := false
	for namespace, fragment := range externals {
		if a.externals[namespace] != fragment {
			changed = true
			break
		}
	}
	maps.Copy(a.externals, externals)
	if changed {
		a.stateVersion++
	}
	a.mu.Unlock()
	logger.Info().Int("count", len(externals)).Msg("Batch loaded external fragments during initialization")
}

func (a *NamespaceAggregator) MarkInitialized() {
	logger := log.With().Str("component", "aggregator").Logger()
	a.mu.Lock()
	changed := a.initializing
	a.initializing = false
	if changed {
		a.stateVersion++
	}
	version := a.stateVersion
	merged := a.currentMergedLocked()
	mirrorUnchanged := version <= a.lastPublishedVersion
	nodeUnchanged := version <= a.lastPushedVersion
	a.mu.Unlock()
	logger.Info().Msg("Aggregator initialization complete, performing initial push")
	if a.publishAggregated && !mirrorUnchanged {
		a.publishMu.Lock()
		a.mu.Lock()
		stalePublish := version < a.stateVersion || version <= a.lastPublishedVersion
		a.mu.Unlock()
		if stalePublish {
			logger.Debug().Msg("Skipping stale mirror publish on initialization")
		} else {
			if err := a.publishMirrorConfigMap(merged); err != nil {
				logger.Error().Err(err).Msg("Failed to publish aggregated ConfigMap on initialization")
			} else {
				a.mu.Lock()
				if version > a.lastPublishedVersion {
					a.lastPublishedToMirror = merged
					a.lastPublishedVersion = version
				}
				a.mu.Unlock()
			}
		}
		a.publishMu.Unlock()
	}
	if nodeUnchanged {
		logger.Debug().Msg("Merged config unchanged for nodes after initialization, skipping push")
		return
	}
	if a.nodeAvailabilityCheck != nil && !a.nodeAvailabilityCheck() {
		logger.Info().Msg("Initialization complete but no nodes available, skipping config push")
		return
	}
	if a.configUpdateHandler != nil {
		a.nodePushMu.Lock()
		a.mu.Lock()
		stalePush := version < a.stateVersion || version <= a.lastPushedVersion
		a.mu.Unlock()
		if stalePush {
			logger.Debug().Msg("Skipping stale node push on initialization")
		} else {
			a.configUpdateHandler(merged)
			a.mu.Lock()
			if version > a.lastPushedVersion {
				a.lastPushedMerged = merged
				a.lastPushedVersion = version
			}
			a.mu.Unlock()
		}
		a.nodePushMu.Unlock()
	}
}

func (a *NamespaceAggregator) RemoveExternal(namespace string) {
	logger := log.With().Str("component", "aggregator").Str("namespace", namespace).Logger()
	a.mu.Lock()
	_, changed := a.externals[namespace]
	if changed {
		delete(a.externals, namespace)
		a.stateVersion++
	}
	version := a.stateVersion
	merged := a.currentMergedLocked()
	mirrorUnchanged := version <= a.lastPublishedVersion
	nodeUnchanged := version <= a.lastPushedVersion
	a.mu.Unlock()
	if a.publishAggregated && !mirrorUnchanged {
		logger.Info().Msg("External fragment removed, publishing to mirror")
		a.publishMu.Lock()
		a.mu.Lock()
		stalePublish := version < a.stateVersion || version <= a.lastPublishedVersion
		a.mu.Unlock()
		if stalePublish {
			logger.Debug().Msg("Skipping stale mirror publish for external removal")
		} else {
			if err := a.publishMirrorConfigMap(merged); err != nil {
				logger.Error().Err(err).Msg("Failed to publish aggregated ConfigMap")
			} else {
				a.mu.Lock()
				if version > a.lastPublishedVersion {
					a.lastPublishedToMirror = merged
					a.lastPublishedVersion = version
				}
				a.mu.Unlock()
			}
		}
		a.publishMu.Unlock()
	}
	if nodeUnchanged {
		logger.Debug().Msg("External fragment removed but merged config unchanged for nodes, skipping push")
		return
	}
	logger.Info().Msg("External fragment removed, pushing to nodes")
	if a.nodeAvailabilityCheck != nil && !a.nodeAvailabilityCheck() {
		logger.Info().Msg("External removed but no nodes available, skipping config push")
		return
	}
	if a.configUpdateHandler != nil {
		a.nodePushMu.Lock()
		a.mu.Lock()
		stalePush := version < a.stateVersion || version <= a.lastPushedVersion
		a.mu.Unlock()
		if stalePush {
			logger.Debug().Msg("Skipping stale node push for external removal")
		} else {
			a.configUpdateHandler(merged)
			a.mu.Lock()
			if version > a.lastPushedVersion {
				a.lastPushedMerged = merged
				a.lastPushedVersion = version
			}
			a.mu.Unlock()
		}
		a.nodePushMu.Unlock()
	}
}

func (a *NamespaceAggregator) currentMergedLocked() string {
	merged := a.base
	if !strings.HasSuffix(merged, "\n") {
		merged += "\n"
	}
	namespaces := make([]string, 0, len(a.externals))
	for ns := range a.externals {
		namespaces = append(namespaces, ns)
	}
	sort.Strings(namespaces)
	for _, ns := range namespaces {
		fragment := a.externals[ns]
		if strings.TrimSpace(fragment) == "" {
			continue
		}
		merged += fmt.Sprintf("\n\n# ---- Begin external from %s ----\n", ns)
		merged += strings.TrimSpace(fragment)
		merged += fmt.Sprintf("\n# ---- End external from %s ----\n", ns)
	}
	return merged
}

func (a *NamespaceAggregator) CurrentMerged() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.currentMergedLocked()
}

func (a *NamespaceAggregator) publishMirrorConfigMap(mergedConfig string) error {
	logger := log.With().Str("component", "aggregator").Str("configmap", a.aggregatedConfigMapName).Logger()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cm, err := a.clientset.CoreV1().ConfigMaps(a.namespace).Get(ctx, a.aggregatedConfigMapName, metav1.GetOptions{})
	if err == nil {
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		cm.Data["Caddyfile"] = mergedConfig
		_, err = a.clientset.CoreV1().ConfigMaps(a.namespace).Update(ctx, cm, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update mirror ConfigMap: %w", err)
		}
		logger.Debug().Msg("Updated mirror ConfigMap")
	} else {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      a.aggregatedConfigMapName,
				Namespace: a.namespace,
				Labels: map[string]string{
					"ckic.cmld.ru/managed": "true",
					"ckic.cmld.ru/type":    "aggregated-config",
				},
			},
			Data: map[string]string{
				"Caddyfile": mergedConfig,
			},
		}
		_, err = a.clientset.CoreV1().ConfigMaps(a.namespace).Create(ctx, cm, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create mirror ConfigMap: %w", err)
		}
		logger.Info().Msg("Created mirror ConfigMap")
	}
	return nil
}
