package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/labels"

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
		Str("key", reconcileKeyLabel(key)).
		Int("requeues", requeues).
		Logger()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		logger.Debug().Err(err).Msg("Reconcile deadline/cancel; will retry")
		return
	}
	logger.Warn().Err(err).Msg("Reconcile failed; requeueing")
}

func reconcileKeyLabel(key string) string {
	if key == configReconcileKey {
		return "config"
	}
	return key
}

func (c *Controller) reconcile(ctx context.Context, key string) error {
	reconcileCtx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	if key == configReconcileKey {
		return c.reconcileConfig(reconcileCtx)
	}
	return c.reconcileNode(reconcileCtx, key)
}

func (c *Controller) reconcileConfig(ctx context.Context) error {
	nodes, err := c.nodeLister.List(c.nodeSelector)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		c.queue.Add(node.Name)
	}
	return c.aggregator.PublishMirror(ctx)
}

func (c *Controller) reconcileNode(ctx context.Context, nodeName string) error {
	node, err := c.nodeLister.Get(nodeName)
	managed := err == nil && c.nodeSelector.Matches(labels.Set(node.Labels))
	if !managed {
		return c.teardownNode(ctx, nodeName)
	}
	instance, err := c.deployFn(
		ctx,
		c.deployOptionsForNode(nodeName),
		nodeName,
		c.config.ExternalEndpoints[nodeName],
	)
	if err != nil {
		return err
	}
	merged := c.aggregator.CurrentMerged()
	digest := configDigest(merged)
	if c.pushUpToDate(nodeName, digest, instance.PodName) {
		return nil
	}
	if !instance.PodReady {
		c.queue.AddAfter(nodeName, podStartupRequeueInterval)
		return nil
	}
	if pushErr := c.pushFn(ctx, instance, merged); pushErr != nil {
		var cfgErr *caddy.ConfigurationFailedError
		if errors.As(pushErr, &cfgErr) && cfgErr.IsPermanent() {
			log.Error().
				Err(pushErr).
				Str("node", nodeName).
				Msg("Caddy rejected the configuration; not retrying until it changes")
			c.recordPush(nodeName, digest, instance.PodName)
			return nil
		}
		return pushErr
	}
	c.recordPush(nodeName, digest, instance.PodName)
	return nil
}

func (c *Controller) deployOptionsForNode(nodeName string) caddy.DeployOptions {
	opts := c.deployOpts
	if c.config.PrePullImage {
		existing, err := c.deployLister.
			Deployments(c.config.Namespace).
			Get("caddy-" + nodeName)
		opts.PrePullImage = err != nil || caddyImageOf(existing) != c.config.CaddyImage
	}
	return opts
}

func caddyImageOf(dep *appsv1.Deployment) string {
	if dep == nil || len(dep.Spec.Template.Spec.Containers) == 0 {
		return ""
	}
	return dep.Spec.Template.Spec.Containers[0].Image
}

func (c *Controller) teardownNode(ctx context.Context, nodeName string) error {
	instance := &caddy.Instance{
		NodeName:       nodeName,
		Namespace:      c.config.Namespace,
		DeploymentName: "caddy-" + nodeName,
		KubeClient:     c.clientset,
	}
	if err := instance.Delete(ctx); err != nil {
		return err
	}
	c.clearPushState(nodeName)
	return nil
}

func (c *Controller) pushUpToDate(nodeName, digest, podName string) bool {
	if podName == "" {
		return false
	}
	c.pushMu.Lock()
	defer c.pushMu.Unlock()
	record, ok := c.pushState[nodeName]
	return ok && record.digest == digest && record.podName == podName
}

func (c *Controller) recordPush(nodeName, digest, podName string) {
	c.pushMu.Lock()
	defer c.pushMu.Unlock()
	c.pushState[nodeName] = pushRecord{digest: digest, podName: podName}
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
	c.queue.Add(configReconcileKey)
}

func configDigest(configData string) string {
	sum := sha256.Sum256([]byte(configData))
	return hex.EncodeToString(sum[:])
}
