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

	"git.horse/vapronva/ckic/pkg/constants"
)

type NamespaceAggregator struct {
	mu                      sync.RWMutex
	publishMu               sync.Mutex
	nodePushMu              sync.Mutex
	inFlightVersion         uint64
	pendingVersion          uint64
	pendingMerged           string
	stateVersion            uint64
	lastPushedVersion       uint64
	lastPublishedVersion    uint64
	base                    string
	externals               map[string]string
	clientset               kubernetes.Interface
	namespace               string
	publishAggregated       bool
	aggregatedConfigMapName string
	configUpdateHandler     func(string)
	nodeAvailabilityCheck   func() bool
	initializing            bool
	lifetimeCtx             context.Context
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
		lifetimeCtx:             context.Background(),
	}
}

func (a *NamespaceAggregator) Attach(ctx context.Context) {
	a.lifetimeCtx = ctx
}

func (a *NamespaceAggregator) ctx() context.Context {
	if a.lifetimeCtx == nil {
		return context.Background()
	}
	return a.lifetimeCtx
}

type dispatchOpts struct {
	skipInitGuard bool
	operation     string
}

func (a *NamespaceAggregator) dispatchAfterMutation(
	snapshot updateSnapshot,
	logger zerolog.Logger,
	opts dispatchOpts,
) {
	if !opts.skipInitGuard && snapshot.initializing {
		logger.Debug().Msg("Updated during initialization, deferring push")
		return
	}
	if snapshot.mirrorDirty {
		a.publishSnapshot(logger, snapshot, opts.operation)
	}
	if !snapshot.nodeDirty {
		logger.Debug().
			Msgf("Merged config unchanged for %s, skipping push", opts.operation)
		return
	}
	if a.nodeAvailabilityCheck != nil && !a.nodeAvailabilityCheck() {
		logger.Info().
			Msgf("No nodes available for %s, skipping config push", opts.operation)
		return
	}
	if a.configUpdateHandler == nil {
		return
	}
	a.pushSnapshotToNodes(logger, snapshot, opts.operation)
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
	a.dispatchAfterMutation(snapshot, logger, dispatchOpts{
		operation: "base update",
	})
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
	a.dispatchAfterMutation(snapshot, logger, dispatchOpts{
		operation: "external update",
	})
}

func (a *NamespaceAggregator) SetExternalBatch(externals map[string]string) {
	logger := log.With().Str("component", "aggregator").Logger()
	snapshot := a.snapshotAfterMutation(func() {
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
	})
	logger.Info().
		Int("count", len(externals)).
		Msg("Batch loaded external fragments")
	a.dispatchAfterMutation(snapshot, logger, dispatchOpts{
		operation: "batch external update",
	})
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
	a.dispatchAfterMutation(snapshot, logger, dispatchOpts{
		skipInitGuard: true,
		operation:     "initialization",
	})
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
	a.dispatchAfterMutation(snapshot, logger, dispatchOpts{
		skipInitGuard: true,
		operation:     "external removal",
	})
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
	operation string,
) {
	if !a.publishAggregated {
		return
	}
	a.publishMu.Lock()
	defer a.publishMu.Unlock()
	if a.isStaleMirrorPublish(snapshot.version) {
		logger.Debug().Msgf("Skipping stale mirror publish for %s", operation)
		return
	}
	if err := a.publishMirrorConfigMap(snapshot.merged); err != nil {
		logger.Error().
			Err(err).
			Msgf("Failed to publish aggregated ConfigMap for %s", operation)
		return
	}
	a.recordMirrorPublish(snapshot.version)
}

func (a *NamespaceAggregator) pushSnapshotToNodes(
	logger zerolog.Logger,
	snapshot updateSnapshot,
	operation string,
) {
	if !a.enqueuePush(snapshot, false) {
		logger.Debug().Msgf("Skipping stale node push for %s", operation)
	}
}

func (a *NamespaceAggregator) enqueuePush(snapshot updateSnapshot, force bool) bool {
	a.nodePushMu.Lock()
	if a.isStaleNodePush(snapshot.version, force) {
		a.nodePushMu.Unlock()
		return false
	}
	if a.inFlightVersion > 0 {
		if snapshot.version > a.inFlightVersion &&
			snapshot.version > a.pendingVersion {
			a.pendingVersion = snapshot.version
			a.pendingMerged = snapshot.merged
		}
		a.nodePushMu.Unlock()
		return true
	}
	a.inFlightVersion = snapshot.version
	a.nodePushMu.Unlock()
	a.drainPushQueue(snapshot)
	return true
}

func (a *NamespaceAggregator) drainPushQueue(initial updateSnapshot) {
	defer func() {
		a.nodePushMu.Lock()
		if a.inFlightVersion != 0 {
			a.inFlightVersion = 0
			a.pendingVersion = 0
			a.pendingMerged = ""
		}
		a.nodePushMu.Unlock()
	}()
	next := initial
	for {
		a.configUpdateHandler(next.merged)
		a.recordNodePush(next.version)
		a.nodePushMu.Lock()
		if a.pendingVersion == 0 {
			a.inFlightVersion = 0
			a.nodePushMu.Unlock()
			return
		}
		next = a.promotePendingLocked()
		a.nodePushMu.Unlock()
	}
}

func (a *NamespaceAggregator) promotePendingLocked() updateSnapshot {
	next := updateSnapshot{version: a.pendingVersion, merged: a.pendingMerged}
	a.inFlightVersion = a.pendingVersion
	a.pendingVersion = 0
	a.pendingMerged = ""
	return next
}

func (a *NamespaceAggregator) isStaleMirrorPublish(version uint64) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return version < a.stateVersion || version <= a.lastPublishedVersion
}

func (a *NamespaceAggregator) isStaleNodePush(version uint64, force bool) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if version < a.stateVersion {
		return true
	}
	if force {
		return version < a.lastPushedVersion
	}
	return version <= a.lastPushedVersion
}

func (a *NamespaceAggregator) recordMirrorPublish(version uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if version <= a.lastPublishedVersion {
		return
	}
	a.lastPublishedVersion = version
}

func (a *NamespaceAggregator) recordNodePush(version uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if version <= a.lastPushedVersion {
		return
	}
	a.lastPushedVersion = version
}

func (a *NamespaceAggregator) currentMergedLocked() string {
	namespaces := make([]string, 0, len(a.externals))
	for ns := range a.externals {
		namespaces = append(namespaces, ns)
	}
	sort.Strings(namespaces)
	var sb strings.Builder
	sb.WriteString(a.base)
	if !strings.HasSuffix(a.base, "\n") {
		sb.WriteByte('\n')
	}
	for _, ns := range namespaces {
		fragment := a.externals[ns]
		if strings.TrimSpace(fragment) == "" {
			continue
		}
		fmt.Fprintf(&sb, "\n\n# ---- Begin external from %s ----\n", ns)
		sb.WriteString(strings.TrimSpace(fragment))
		fmt.Fprintf(&sb, "\n# ---- End external from %s ----\n", ns)
	}
	return sb.String()
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
	a.mu.RLock()
	version := a.stateVersion
	merged := a.currentMergedLocked()
	handler := a.configUpdateHandler
	a.mu.RUnlock()
	if handler == nil {
		return
	}
	if a.enqueuePush(updateSnapshot{version: version, merged: merged}, true) {
		logger.Info().Msg("Forced sync queued current merged config for nodes")
		return
	}
	logger.Debug().
		Msg("Forced sync snapshot superseded by newer state; active dispatch will deliver")
}

func (a *NamespaceAggregator) publishMirrorConfigMap(mergedConfig string) error {
	logger := log.With().
		Str("component", "aggregator").
		Str("configmap", a.aggregatedConfigMapName).
		Logger()
	ctx, cancel := context.WithTimeout(a.ctx(), mirrorPublishTimeout)
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
				Labels:    constants.AggregatedConfigLabels(),
			},
			Data: map[string]string{
				constants.CaddyfileKey: mergedConfig,
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
	cm.Data[constants.CaddyfileKey] = mergedConfig
	_, err = a.clientset.CoreV1().
		ConfigMaps(a.namespace).
		Update(ctx, cm, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update mirror ConfigMap: %w", err)
	}
	logger.Debug().Msg("Updated mirror ConfigMap")
	return nil
}
