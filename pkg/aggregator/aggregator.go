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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	mu          sync.RWMutex
	publishMu   sync.Mutex
	base        string
	hasBase     bool
	accepted    string
	hasAccepted bool
	externals   map[string]string
	clientset   kubernetes.Interface
	namespace   string
	mirrorName  string
	enqueue     func()
}

func New(
	clientset kubernetes.Interface,
	namespace string,
	mirrorName string,
	enqueue func(),
) *Aggregator {
	return &Aggregator{
		externals:  make(map[string]string),
		clientset:  clientset,
		namespace:  namespace,
		mirrorName: mirrorName,
		enqueue:    enqueue,
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
	merged := sb.String()
	if len(merged) > corev1.MaxSecretSize {
		return "", fmt.Errorf("merged Caddyfile is %d bytes, exceeding the ConfigMap limit of %d", len(merged), corev1.MaxSecretSize)
	}
	return merged, nil
}

func (a *Aggregator) InitializeMirror(ctx context.Context, bootstrap string) error {
	ctx, cancel := context.WithTimeout(ctx, mirrorPublishTimeout)
	defer cancel()
	configMaps := a.clientset.CoreV1().ConfigMaps(a.namespace)
	mirror, err := configMaps.Get(ctx, a.mirrorName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		mirror, err = configMaps.Create(ctx, &corev1.ConfigMap{
			Name: a.mirrorName, Namespace: a.namespace,
			Labels: constants.AggregatedConfigLabels(),
			Data:   map[string]string{constants.CaddyfileKey: bootstrap},
		}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			mirror, err = configMaps.Get(ctx, a.mirrorName, metav1.GetOptions{})
		}
	}
	if err != nil {
		return fmt.Errorf("failed to initialize boot ConfigMap: %w", err)
	}
	accepted := mirror.Data[constants.CaddyfileKey]
	if strings.TrimSpace(accepted) == "" {
		accepted = bootstrap
		if err := a.publishMirror(ctx, accepted); err != nil {
			return err
		}
	}
	a.mu.Lock()
	a.accepted = accepted
	a.hasAccepted = true
	a.mu.Unlock()
	return nil
}

func (a *Aggregator) CurrentAccepted() (string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.hasAccepted {
		return "", errors.New("boot ConfigMap has not been initialized")
	}
	return a.accepted, nil
}

func (a *Aggregator) PublishAccepted(ctx context.Context, accepted string) error {
	a.publishMu.Lock()
	defer a.publishMu.Unlock()
	if current, err := a.CurrentAccepted(); err == nil && current == accepted {
		return nil
	}
	merged, err := a.CurrentMerged()
	if err != nil {
		return err
	}
	if merged != accepted {
		return nil
	}
	a.mu.Lock()
	a.accepted = accepted
	a.hasAccepted = true
	a.mu.Unlock()
	return a.publishMirror(ctx, accepted)
}

func (a *Aggregator) PublishMirror(ctx context.Context) error {
	a.publishMu.Lock()
	defer a.publishMu.Unlock()
	accepted, err := a.CurrentAccepted()
	if err != nil {
		return err
	}
	return a.publishMirror(ctx, accepted)
}

func (a *Aggregator) publishMirror(ctx context.Context, accepted string) error {
	apply := corev1ac.ConfigMap(a.mirrorName, a.namespace).
		WithLabels(constants.AggregatedConfigLabels()).
		WithData(map[string]string{constants.CaddyfileKey: accepted})
	ctx, cancel := context.WithTimeout(ctx, mirrorPublishTimeout)
	defer cancel()
	if _, err := a.clientset.CoreV1().ConfigMaps(a.namespace).Apply(
		ctx, apply, metav1.ApplyOptions{FieldManager: mirrorFieldManager, Force: true},
	); err != nil {
		return fmt.Errorf("failed to publish mirror ConfigMap: %w", err)
	}
	return nil
}
