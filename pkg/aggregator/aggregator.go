package aggregator

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	clientset               kubernetes.Interface
	namespace               string
	publishAggregated       bool
	aggregatedConfigMapName string
	configUpdateHandler     func(string)
	nodeAvailabilityCheck   func() bool
	initializing            bool
}

const mirrorPublishTimeout = 30 * time.Second

type updateSnapshot struct {
	version      uint64
	merged       string
	initializing bool
	mirrorDirty  bool
	nodeDirty    bool
}

func NewNamespaceAggregator(
	clientset kubernetes.Interface,
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
	snapshot := a.snapshotAfterMutation(func() {
		changed := a.base != base
		a.base = base
		if changed {
			a.stateVersion++
		}
	})
	if snapshot.initializing {
		logger.Debug().Msg("Base updated during initialization, deferring push")
		return
	}
	if snapshot.mirrorDirty {
		a.publishSnapshot(
			logger,
			snapshot,
			"Skipping stale mirror publish for base update",
			"Failed to publish aggregated ConfigMap",
		)
	}
	if !snapshot.nodeDirty {
		logger.Debug().
			Msg("Base updated but merged config unchanged for nodes, skipping push")
		return
	}
	if !a.canPushToNodes(
		logger,
		"Base updated but no nodes available, skipping config push",
	) {
		return
	}
	a.pushSnapshotToNodes(
		logger,
		snapshot,
		"Skipping stale node push for base update",
	)
}

func (a *NamespaceAggregator) SetExternal(namespace, fragment string) {
	logger := log.With().
		Str("component", "aggregator").
		Str("namespace", namespace).
		Logger()
	snapshot := a.snapshotAfterMutation(func() {
		changed := a.externals[namespace] != fragment
		a.externals[namespace] = fragment
		if changed {
			a.stateVersion++
		}
	})
	if snapshot.initializing {
		logger.Debug().
			Msg("External fragment updated during initialization, deferring push")
		return
	}
	if snapshot.mirrorDirty {
		logger.Info().Msg("External fragment updated, publishing to mirror")
		a.publishSnapshot(
			logger,
			snapshot,
			"Skipping stale mirror publish for external update",
			"Failed to publish aggregated ConfigMap",
		)
	}
	if !snapshot.nodeDirty {
		logger.Debug().
			Msg("External fragment updated but merged config unchanged for nodes, skipping push")
		return
	}
	logger.Info().Msg("External fragment updated, pushing to nodes")
	if !a.canPushToNodes(
		logger,
		"External updated but no nodes available, skipping config push",
	) {
		return
	}
	a.pushSnapshotToNodes(
		logger,
		snapshot,
		"Skipping stale node push for external update",
	)
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
	logger.Info().
		Int("count", len(externals)).
		Msg("Batch loaded external fragments during initialization")
}

func (a *NamespaceAggregator) MarkInitialized() {
	logger := log.With().Str("component", "aggregator").Logger()
	snapshot := a.snapshotAfterMutation(func() {
		changed := a.initializing
		a.initializing = false
		if changed {
			a.stateVersion++
		}
	})
	logger.Info().Msg("Aggregator initialization complete, performing initial push")
	if snapshot.mirrorDirty {
		a.publishSnapshot(
			logger,
			snapshot,
			"Skipping stale mirror publish on initialization",
			"Failed to publish aggregated ConfigMap on initialization",
		)
	}
	if !snapshot.nodeDirty {
		logger.Debug().
			Msg("Merged config unchanged for nodes after initialization, skipping push")
		return
	}
	if !a.canPushToNodes(
		logger,
		"Initialization complete but no nodes available, skipping config push",
	) {
		return
	}
	a.pushSnapshotToNodes(
		logger,
		snapshot,
		"Skipping stale node push on initialization",
	)
}

func (a *NamespaceAggregator) RemoveExternal(namespace string) {
	logger := log.With().
		Str("component", "aggregator").
		Str("namespace", namespace).
		Logger()
	snapshot := a.snapshotAfterMutation(func() {
		if _, changed := a.externals[namespace]; changed {
			delete(a.externals, namespace)
			a.stateVersion++
		}
	})
	if snapshot.mirrorDirty {
		logger.Info().Msg("External fragment removed, publishing to mirror")
		a.publishSnapshot(
			logger,
			snapshot,
			"Skipping stale mirror publish for external removal",
			"Failed to publish aggregated ConfigMap",
		)
	}
	if !snapshot.nodeDirty {
		logger.Debug().
			Msg("External fragment removed but merged config unchanged for nodes, skipping push")
		return
	}
	logger.Info().Msg("External fragment removed, pushing to nodes")
	if !a.canPushToNodes(
		logger,
		"External removed but no nodes available, skipping config push",
	) {
		return
	}
	a.pushSnapshotToNodes(
		logger,
		snapshot,
		"Skipping stale node push for external removal",
	)
}

func (a *NamespaceAggregator) snapshotAfterMutation(
	mutator func(),
) updateSnapshot {
	a.mu.Lock()
	mutator()
	snapshot := updateSnapshot{
		version:      a.stateVersion,
		merged:       a.currentMergedLocked(),
		initializing: a.initializing,
		mirrorDirty:  a.stateVersion > a.lastPublishedVersion,
		nodeDirty:    a.stateVersion > a.lastPushedVersion,
	}
	a.mu.Unlock()
	return snapshot
}

func (a *NamespaceAggregator) publishSnapshot(
	logger zerolog.Logger,
	snapshot updateSnapshot,
	staleMsg, errorMsg string,
) {
	if !a.publishAggregated {
		return
	}
	a.publishMu.Lock()
	defer a.publishMu.Unlock()
	if a.isStaleMirrorPublish(snapshot.version) {
		logger.Debug().Msg(staleMsg)
		return
	}
	if err := a.publishMirrorConfigMap(snapshot.merged); err != nil {
		logger.Error().Err(err).Msg(errorMsg)
		return
	}
	a.recordMirrorPublish(snapshot.version, snapshot.merged)
}

func (a *NamespaceAggregator) canPushToNodes(
	logger zerolog.Logger,
	noNodesMsg string,
) bool {
	if a.nodeAvailabilityCheck != nil && !a.nodeAvailabilityCheck() {
		logger.Info().Msg(noNodesMsg)
		return false
	}
	return a.configUpdateHandler != nil
}

func (a *NamespaceAggregator) pushSnapshotToNodes(
	logger zerolog.Logger,
	snapshot updateSnapshot,
	staleMsg string,
) {
	a.nodePushMu.Lock()
	defer a.nodePushMu.Unlock()
	if a.isStaleNodePush(snapshot.version) {
		logger.Debug().Msg(staleMsg)
		return
	}
	a.configUpdateHandler(snapshot.merged)
	a.recordNodePush(snapshot.version, snapshot.merged)
}

func (a *NamespaceAggregator) isStaleMirrorPublish(version uint64) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return version < a.stateVersion || version <= a.lastPublishedVersion
}

func (a *NamespaceAggregator) isStaleNodePush(version uint64) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return version < a.stateVersion || version <= a.lastPushedVersion
}

func (a *NamespaceAggregator) recordMirrorPublish(version uint64, merged string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if version <= a.lastPublishedVersion {
		return
	}
	a.lastPublishedToMirror = merged
	a.lastPublishedVersion = version
}

func (a *NamespaceAggregator) recordNodePush(version uint64, merged string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if version <= a.lastPushedVersion {
		return
	}
	a.lastPushedMerged = merged
	a.lastPushedVersion = version
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
	var mergedSb strings.Builder
	for _, ns := range namespaces {
		fragment := a.externals[ns]
		if strings.TrimSpace(fragment) == "" {
			continue
		}
		fmt.Fprintf(&mergedSb, "\n\n# ---- Begin external from %s ----\n", ns)
		mergedSb.WriteString(strings.TrimSpace(fragment))
		fmt.Fprintf(&mergedSb, "\n# ---- End external from %s ----\n", ns)
	}
	merged += mergedSb.String()
	return merged
}

func (a *NamespaceAggregator) CurrentMerged() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.currentMergedLocked()
}

func (a *NamespaceAggregator) EnsureNodeSync() {
	logger := log.With().Str("component", "aggregator").Logger()
	if a.nodeAvailabilityCheck != nil && !a.nodeAvailabilityCheck() {
		logger.Debug().
			Msg("Forced node sync requested but no nodes available, skipping push")
		return
	}
	a.nodePushMu.Lock()
	defer a.nodePushMu.Unlock()
	a.mu.RLock()
	version := a.stateVersion
	merged := a.currentMergedLocked()
	handler := a.configUpdateHandler
	a.mu.RUnlock()
	if handler == nil {
		return
	}
	handler(merged)
	a.mu.Lock()
	a.lastPushedMerged = merged
	if version > a.lastPushedVersion {
		a.lastPushedVersion = version
	}
	a.mu.Unlock()
	logger.Info().Msg("Forced sync pushed current merged config to nodes")
}

func (a *NamespaceAggregator) publishMirrorConfigMap(mergedConfig string) error {
	logger := log.With().
		Str("component", "aggregator").
		Str("configmap", a.aggregatedConfigMapName).
		Logger()
	ctx, cancel := context.WithTimeout(context.Background(), mirrorPublishTimeout)
	defer cancel()
	cm, err := a.clientset.CoreV1().
		ConfigMaps(a.namespace).
		Get(ctx, a.aggregatedConfigMapName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get mirror ConfigMap: %w", err)
		}
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
		_, err = a.clientset.CoreV1().
			ConfigMaps(a.namespace).
			Create(ctx, cm, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create mirror ConfigMap: %w", err)
		}
		logger.Info().Msg("Created mirror ConfigMap")
		return nil
	}
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data["Caddyfile"] = mergedConfig
	_, err = a.clientset.CoreV1().
		ConfigMaps(a.namespace).
		Update(ctx, cm, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update mirror ConfigMap: %w", err)
	}
	logger.Debug().Msg("Updated mirror ConfigMap")
	return nil
}
