package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/rs/zerolog/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"git.horse/vapronva/ckic/pkg/aggregator"
	"git.horse/vapronva/ckic/pkg/caddy"
)

func (c *Controller) processNextItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)
	if err := c.reconcile(ctx, key); err != nil {
		c.queue.AddRateLimited(key)
		logReconcileError(key, err, c.queue.NumRequeues(key))
		return true
	}
	c.queue.Forget(key)
	return true
}

func logReconcileError(key string, err error, requeues int) {
	logger := log.With().
		Str("key", key).
		Int("requeues", requeues).
		Logger()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		logger.Debug().Err(err).Msg("Reconcile deadline/cancel; will retry")
		return
	}
	logger.Warn().Err(err).Msg("Reconcile failed; requeueing")
}

func (c *Controller) reconcile(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	if key == mirrorKey {
		return c.aggregator.PublishMirror(ctx)
	}
	return c.reconcileNode(ctx, key)
}

func (c *Controller) reconcileNode(ctx context.Context, nodeName string) error {
	node, err := c.nodeLister.Get(nodeName)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err != nil || !c.isManagedNode(node) {
		return c.teardownNode(ctx, nodeName)
	}
	pod, err := c.deployFn(ctx, node)
	if err != nil {
		return err
	}
	snapshot, err := c.aggregator.CurrentMerged()
	if err != nil {
		log.Warn().Err(err).Str("node", nodeName).Msg("No loadable Caddy configuration; waiting for a ConfigMap change")
		return nil
	}
	isLoaded, err := c.ensureLoaded(ctx, nodeName, pod, snapshot)
	if err != nil || !isLoaded {
		return err
	}
	if c.aggregator.Accept(snapshot) {
		c.queue.Add(mirrorKey)
	}
	return nil
}

func (c *Controller) ensureLoaded(ctx context.Context, nodeName string, pod caddy.Pod, snapshot aggregator.Snapshot) (bool, error) {
	digest := configDigest(snapshot.Caddyfile)
	if c.pushUpToDate(nodeName, digest, pod.ContainerID) {
		return true, nil
	}
	if pod.IP == "" || pod.ContainerID == "" {
		log.Debug().Str("node", nodeName).Str("pod", pod.Name).Msg("Caddy container has not started yet")
		return false, nil
	}
	err := c.pushFn(ctx, pod.IP, snapshot.Caddyfile)
	if errors.Is(err, caddy.ErrConfigRejected) {
		log.Error().Err(err).Str("node", nodeName).Str("pod", pod.Name).
			Msg("Caddy rejected the configuration; retrying on the next change or resync")
		return false, nil
	}
	if err != nil {
		return false, err
	}
	log.Info().Str("node", nodeName).Str("pod", pod.Name).Msg("Updated Caddy configuration")
	c.recordPush(nodeName, digest, pod.ContainerID)
	return true, nil
}

func (c *Controller) teardownNode(ctx context.Context, nodeName string) error {
	if err := caddy.Teardown(ctx, c.config.Deploy, nodeName); err != nil {
		return err
	}
	c.clearPushState(nodeName)
	return nil
}

func (c *Controller) pushUpToDate(nodeName, digest, containerID string) bool {
	if containerID == "" {
		return false
	}
	c.pushMu.Lock()
	defer c.pushMu.Unlock()
	record, ok := c.pushState[nodeName]
	return ok && record.digest == digest && record.containerID == containerID
}

func (c *Controller) recordPush(nodeName, digest, containerID string) {
	c.pushMu.Lock()
	defer c.pushMu.Unlock()
	c.pushState[nodeName] = pushRecord{digest: digest, containerID: containerID}
}

func (c *Controller) clearPushState(nodeName string) {
	c.pushMu.Lock()
	defer c.pushMu.Unlock()
	delete(c.pushState, nodeName)
}

func (c *Controller) forceConfigResync() {
	c.pushMu.Lock()
	clear(c.pushState)
	c.pushMu.Unlock()
	c.enqueueManagedNodes()
}

func configDigest(configData string) string {
	sum := sha256.Sum256([]byte(configData))
	return hex.EncodeToString(sum[:])
}
