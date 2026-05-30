package aggregator

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/constants"
)

const (
	mirrorPublishTimeout = 30 * time.Second
	mirrorFieldManager   = "ckic-aggregator"
)

type Aggregator struct {
	mu                sync.RWMutex
	base              string
	externals         map[string]string
	clientset         kubernetes.Interface
	namespace         string
	publishAggregated bool
	mirrorName        string
	enqueue           func()
}

func New(
	clientset kubernetes.Interface,
	namespace string,
	publishAggregated bool,
	mirrorName string,
	enqueue func(),
) *Aggregator {
	return &Aggregator{
		externals:         make(map[string]string),
		clientset:         clientset,
		namespace:         namespace,
		publishAggregated: publishAggregated,
		mirrorName:        mirrorName,
		enqueue:           enqueue,
	}
}

func (a *Aggregator) notifyIfChanged(changed bool) {
	if changed && a.enqueue != nil {
		a.enqueue()
	}
}

func (a *Aggregator) UpdateBase(base string) {
	a.mu.Lock()
	changed := a.base != base
	a.base = base
	a.mu.Unlock()
	a.notifyIfChanged(changed)
}

func (a *Aggregator) SetExternal(source, fragment string) {
	a.mu.Lock()
	changed := a.externals[source] != fragment
	a.externals[source] = fragment
	a.mu.Unlock()
	a.notifyIfChanged(changed)
}

func (a *Aggregator) SetExternalBatch(externals map[string]string) {
	a.mu.Lock()
	changed := false
	for source, fragment := range externals {
		if a.externals[source] != fragment {
			changed = true
		}
	}
	maps.Copy(a.externals, externals)
	a.mu.Unlock()
	a.notifyIfChanged(changed)
}

func (a *Aggregator) RemoveExternal(source string) {
	a.mu.Lock()
	_, changed := a.externals[source]
	delete(a.externals, source)
	a.mu.Unlock()
	a.notifyIfChanged(changed)
}

func (a *Aggregator) CurrentMerged() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	sources := make([]string, 0, len(a.externals))
	for source := range a.externals {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	var sb strings.Builder
	sb.WriteString(a.base)
	if !strings.HasSuffix(a.base, "\n") {
		sb.WriteByte('\n')
	}
	for _, source := range sources {
		fragment := strings.TrimSpace(a.externals[source])
		if fragment == "" {
			continue
		}
		fmt.Fprintf(&sb, "\n\n# ---- Begin external from %s ----\n", source)
		sb.WriteString(fragment)
		fmt.Fprintf(&sb, "\n# ---- End external from %s ----\n", source)
	}
	return sb.String()
}

func (a *Aggregator) PublishMirror(ctx context.Context) error {
	if !a.publishAggregated {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, mirrorPublishTimeout)
	defer cancel()
	merged := a.CurrentMerged()
	apply := corev1ac.ConfigMap(a.mirrorName, a.namespace).
		WithLabels(constants.AggregatedConfigLabels()).
		WithData(map[string]string{constants.CaddyfileKey: merged})
	if _, err := a.clientset.CoreV1().ConfigMaps(a.namespace).Apply(
		ctx, apply, metav1.ApplyOptions{FieldManager: mirrorFieldManager, Force: true},
	); err != nil {
		return fmt.Errorf("failed to publish mirror ConfigMap: %w", err)
	}
	return nil
}
