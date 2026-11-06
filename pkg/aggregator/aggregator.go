package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/caddy"
)

const (
	mirrorPublishTimeout   = 30 * time.Second
	mirrorFieldManager     = "ckic-aggregator"
	annotationSnapshotHash = "ckic.cmld.ru/snapshot-hash"
)

type Snapshot struct {
	Caddyfile string
	Revision  uint64
}

type Aggregator struct {
	mu               sync.RWMutex
	base             string
	hasBase          bool
	revision         uint64
	accepted         string
	acceptedRevision uint64
	externals        map[string]string
	clientset        kubernetes.Interface
	namespace        string
	mirrorName       string
	bootstrap        string
	enqueue          func()
}

func New(
	clientset kubernetes.Interface,
	namespace string,
	mirrorName string,
	bootstrap string,
	enqueue func(),
) *Aggregator {
	return &Aggregator{
		externals:  make(map[string]string),
		clientset:  clientset,
		namespace:  namespace,
		mirrorName: mirrorName,
		bootstrap:  bootstrap,
		enqueue:    enqueue,
	}
}

func (a *Aggregator) update(mutate func() bool) {
	a.mu.Lock()
	changed := mutate()
	if changed {
		a.revision++
	}
	a.mu.Unlock()
	if changed && a.enqueue != nil {
		a.enqueue()
	}
}

func (a *Aggregator) UpdateBase(base string) {
	a.update(func() bool {
		changed := !a.hasBase || a.base != base
		a.base = base
		a.hasBase = true
		return changed
	})
}

func (a *Aggregator) SetExternal(source, fragment string) {
	a.update(func() bool {
		changed := a.externals[source] != fragment
		a.externals[source] = fragment
		return changed
	})
}

func (a *Aggregator) RemoveExternal(source string) {
	a.update(func() bool {
		_, changed := a.externals[source]
		delete(a.externals, source)
		return changed
	})
}

func (a *Aggregator) CurrentMerged() (Snapshot, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.hasBase {
		return Snapshot{}, errors.New("base ConfigMap has not supplied a Caddyfile")
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
		return Snapshot{}, fmt.Errorf("merged Caddyfile is %d bytes, exceeding the ConfigMap limit of %d", len(merged), corev1.MaxSecretSize)
	}
	return Snapshot{Caddyfile: merged, Revision: a.revision}, nil
}

func (a *Aggregator) InitializeMirror(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, mirrorPublishTimeout)
	defer cancel()
	mirror, err := a.readMirror(ctx)
	if err != nil {
		return err
	}
	accepted := mirror.Data[caddy.CaddyfileKey]
	if mirror.Annotations[annotationSnapshotHash] != a.snapshotHash(accepted) {
		accepted = a.bootstrap
		if err := a.publishMirror(ctx, accepted); err != nil {
			return err
		}
		log.Info().Str("configmap", a.mirrorName).Msg("Published the bootstrap Caddyfile as the boot snapshot")
	}
	a.mu.Lock()
	a.accepted = accepted
	a.mu.Unlock()
	return nil
}

func (a *Aggregator) snapshotHash(caddyfile string) string {
	digest := sha256.Sum256([]byte(a.bootstrap + "\x00" + caddyfile))
	return hex.EncodeToString(digest[:8])
}

func (a *Aggregator) readMirror(ctx context.Context) (*corev1.ConfigMap, error) {
	mirror, err := a.clientset.CoreV1().ConfigMaps(a.namespace).Get(ctx, a.mirrorName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return &corev1.ConfigMap{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read boot ConfigMap %s: %w", a.mirrorName, err)
	}
	return mirror, nil
}

func (a *Aggregator) IsMirrorCurrent(mirror *corev1.ConfigMap) bool {
	caddyfile := mirror.Data[caddy.CaddyfileKey]
	return caddyfile == a.currentAccepted() && mirror.Annotations[annotationSnapshotHash] == a.snapshotHash(caddyfile)
}

func (a *Aggregator) currentAccepted() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.accepted
}

func (a *Aggregator) Accept(snapshot Snapshot) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if snapshot.Revision <= a.acceptedRevision {
		return false
	}
	a.accepted = snapshot.Caddyfile
	a.acceptedRevision = snapshot.Revision
	return true
}

func (a *Aggregator) PublishMirror(ctx context.Context) error {
	accepted := a.currentAccepted()
	if accepted == "" {
		return fmt.Errorf("boot ConfigMap %s has not been initialized", a.mirrorName)
	}
	return a.publishMirror(ctx, accepted)
}

func (a *Aggregator) publishMirror(ctx context.Context, accepted string) error {
	apply := corev1ac.ConfigMap(a.mirrorName, a.namespace).
		WithLabels(caddy.BootConfigLabels()).
		WithAnnotations(map[string]string{annotationSnapshotHash: a.snapshotHash(accepted)}).
		WithData(map[string]string{caddy.CaddyfileKey: accepted})
	ctx, cancel := context.WithTimeout(ctx, mirrorPublishTimeout)
	defer cancel()
	if _, err := a.clientset.CoreV1().ConfigMaps(a.namespace).Apply(
		ctx, apply, metav1.ApplyOptions{FieldManager: mirrorFieldManager, Force: true},
	); err != nil {
		return fmt.Errorf("failed to publish boot ConfigMap %s: %w", a.mirrorName, err)
	}
	return nil
}
