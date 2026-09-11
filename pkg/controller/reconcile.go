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
	if key == mirrorRepairKey {
		return "mirror"
	}
	return key
}

func (c *Controller) reconcile(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	if key == mirrorRepairKey {
		return c.aggregator.PublishMirror(ctx)
	}
	return c.reconcileNode(ctx, key)
}

func (c *Controller) reconcileNode(ctx context.Context, nodeName string) error {
	node, err := c.nodeLister.Get(nodeName)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	isManaged := err == nil && c.nodeSelector.Matches(labels.Set(node.Labels))
	if !isManaged {
		return c.teardownNode(ctx, nodeName)
	}
	snapshot, err := c.aggregator.CurrentMerged()
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
	isLoaded, err := c.ensureLoaded(ctx, nodeName, instance, snapshot)
	if err != nil || !isLoaded {
		return err
	}
	return c.aggregator.PublishAccepted(ctx, snapshot)
}

func (c *Controller) ensureLoaded(
	ctx context.Context,
	nodeName string,
	instance *caddy.Instance,
	snapshot aggregator.Snapshot,
) (bool, error) {
	digest := configDigest(snapshot.Caddyfile)
	if c.pushUpToDate(nodeName, digest, instance.ContainerID) {
		return true, nil
	}
	if instance.PodIP == "" || instance.ContainerID == "" {
		c.queue.AddAfter(nodeName, podStartupRequeueInterval)
		return false, nil
	}
	if err := c.pushFn(ctx, instance, snapshot.Caddyfile); err != nil {
		return false, err
	}
	c.recordPush(nodeName, digest, instance.ContainerID)
	return true, nil
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
	c.enqueueManagedNodes()
}

func configDigest(configData string) string {
	sum := sha256.Sum256([]byte(configData))
	return hex.EncodeToString(sum[:])
}
