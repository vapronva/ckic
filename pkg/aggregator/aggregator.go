package aggregator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
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
	hasBase           bool
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
	changed := !a.hasBase || a.base != base
	a.base = base
	a.hasBase = true
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

func (a *Aggregator) RemoveExternal(source string) {
	a.mu.Lock()
	_, changed := a.externals[source]
	delete(a.externals, source)
	a.mu.Unlock()
	a.notifyIfChanged(changed)
}

func (a *Aggregator) CurrentMerged() (string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.hasBase {
		return "", errors.New("base ConfigMap has not supplied a Caddyfile")
	}
	var sb strings.Builder
	sb.WriteString(a.base)
	if !strings.HasSuffix(a.base, "\n") {
		sb.WriteByte('\n')
	}
	for _, source := range slices.Sorted(maps.Keys(a.externals)) {
		fragment := strings.TrimSpace(a.externals[source])
		if fragment == "" {
			continue
		}
		fmt.Fprintf(&sb, "\n\n# ---- Begin external from %s ----\n", source)
		sb.WriteString(fragment)
		fmt.Fprintf(&sb, "\n# ---- End external from %s ----\n", source)
	}
	return sb.String(), nil
}

func (a *Aggregator) PublishMirror(ctx context.Context) error {
	if !a.publishAggregated {
		return nil
	}
	merged, err := a.CurrentMerged()
	if err != nil {
		return err
	}
	if len(merged) > corev1.MaxSecretSize {
		return fmt.Errorf("merged Caddyfile is %d bytes, exceeding the ConfigMap limit of %d", len(merged), corev1.MaxSecretSize)
	}
	apply := corev1ac.ConfigMap(a.mirrorName, a.namespace).
		WithLabels(constants.AggregatedConfigLabels()).
		WithData(map[string]string{constants.CaddyfileKey: merged})
	ctx, cancel := context.WithTimeout(ctx, mirrorPublishTimeout)
	defer cancel()
	if _, err := a.clientset.CoreV1().ConfigMaps(a.namespace).Apply(
		ctx, apply, metav1.ApplyOptions{FieldManager: mirrorFieldManager, Force: true},
	); err != nil {
		return fmt.Errorf("failed to publish mirror ConfigMap: %w", err)
	}
	return nil
}
