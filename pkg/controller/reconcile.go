package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	managed := err == nil && c.nodeSelector.Matches(labels.Set(node.Labels))
	if !managed {
		return c.teardownNode(ctx, nodeName)
	}
	merged, err := c.aggregator.CurrentMerged()
	if err != nil {
		return err
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
	digest := configDigest(merged)
	if !c.pushUpToDate(nodeName, digest, instance.ContainerID) {
		if instance.PodIP == "" || instance.ContainerID == "" {
			c.queue.AddAfter(nodeName, podStartupRequeueInterval)
			return nil
		}
		if err := c.pushFn(ctx, instance, merged); err != nil {
			return err
		}
		c.recordPush(nodeName, digest, instance.ContainerID)
	}
	if err := c.aggregator.PublishAccepted(ctx, merged); err != nil {
		c.queue.Add(configReconcileKey)
		return err
	}
	return nil
}

func (c *Controller) deployOptionsForNode(nodeName string) caddy.DeployOptions {
	opts := c.deployOpts
	if opts.PrePullImage {
		existing, err := c.deployLister.Deployments(c.config.Namespace).Get(caddy.DeploymentName(nodeName))
		opts.PrePullImage = err != nil || caddyImageOf(existing) != opts.CaddyImage
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
		DeploymentName: caddy.DeploymentName(nodeName),
		KubeClient:     c.clientset,
	}
	if err := instance.Delete(ctx); err != nil {
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
	c.queue.Add(configReconcileKey)
}

func configDigest(configData string) string {
	sum := sha256.Sum256([]byte(configData))
	return hex.EncodeToString(sum[:])
}
