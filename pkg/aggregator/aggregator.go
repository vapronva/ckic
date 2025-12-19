package aggregator

import (
	"context"
	"fmt"
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
	base                    string
	externals               map[string]string
	clientset               *kubernetes.Clientset
	namespace               string
	publishAggregated       bool
	aggregatedConfigMapName string
	configUpdateHandler     func(string)
	nodeAvailabilityCheck   func() bool
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
	}
}

func (a *NamespaceAggregator) UpdateBase(base string) {
	logger := log.With().Str("component", "aggregator").Logger()
	a.mu.Lock()
	a.base = base
	merged := a.currentMergedLocked()
	a.mu.Unlock()
	if a.publishAggregated {
		if err := a.publishMirrorConfigMap(merged); err != nil {
			logger.Error().Err(err).Msg("Failed to publish aggregated ConfigMap")
		}
	}
	if a.nodeAvailabilityCheck != nil && !a.nodeAvailabilityCheck() {
		logger.Info().Msg("Base updated but no nodes available, skipping config push")
		return
	}
	if a.configUpdateHandler != nil {
		a.configUpdateHandler(merged)
	}
}

func (a *NamespaceAggregator) SetExternal(namespace, fragment string) {
	logger := log.With().Str("component", "aggregator").Str("namespace", namespace).Logger()
	a.mu.Lock()
	a.externals[namespace] = fragment
	merged := a.currentMergedLocked()
	a.mu.Unlock()
	logger.Info().Msg("External fragment updated")
	if a.publishAggregated {
		if err := a.publishMirrorConfigMap(merged); err != nil {
			logger.Error().Err(err).Msg("Failed to publish aggregated ConfigMap")
		}
	}
	if a.nodeAvailabilityCheck != nil && !a.nodeAvailabilityCheck() {
		logger.Info().Msg("External updated but no nodes available, skipping config push")
		return
	}
	if a.configUpdateHandler != nil {
		a.configUpdateHandler(merged)
	}
}

func (a *NamespaceAggregator) RemoveExternal(namespace string) {
	logger := log.With().Str("component", "aggregator").Str("namespace", namespace).Logger()
	a.mu.Lock()
	delete(a.externals, namespace)
	merged := a.currentMergedLocked()
	a.mu.Unlock()
	logger.Info().Msg("External fragment removed")
	if a.publishAggregated {
		if err := a.publishMirrorConfigMap(merged); err != nil {
			logger.Error().Err(err).Msg("Failed to publish aggregated ConfigMap")
		}
	}
	if a.nodeAvailabilityCheck != nil && !a.nodeAvailabilityCheck() {
		logger.Info().Msg("External removed but no nodes available, skipping config push")
		return
	}
	if a.configUpdateHandler != nil {
		a.configUpdateHandler(merged)
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
