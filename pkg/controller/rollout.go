package controller

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"git.horse/vapronva/ckic/pkg/caddy"
)

var stuckPrePullSampler = &zerolog.BurstSampler{Burst: 1, Period: time.Minute}

type nodeRollout struct {
	name         string
	isPending    bool
	isAvailable  bool
	needsPrePull bool
	prePull      *corev1.Pod
}

func (c *Controller) deployNode(ctx context.Context, nodeName string) (caddy.Pod, error) {
	isHeld, err := c.syncRollout(ctx, nodeName)
	if err != nil {
		return caddy.Pod{}, err
	}
	if isHeld {
		c.queue.AddAfter(nodeName, requeueInterval)
	}
	opts, externalIPs := c.config.Deploy, c.config.ExternalEndpoints[nodeName]
	switch {
	case !isHeld:
		err = caddy.EnsureCaddy(ctx, opts, nodeName, externalIPs)
	case opts.EnableCiliumLB:
		err = caddy.ApplyLoadBalancerService(ctx, opts, nodeName, externalIPs)
	}
	if err != nil {
		return caddy.Pod{}, err
	}
	return caddy.FindActivePod(ctx, opts, nodeName)
}

func (c *Controller) syncRollout(ctx context.Context, nodeName string) (bool, error) {
	self := c.nodeRollout(nodeName)
	if err := c.syncPrePullPod(ctx, self, time.Now()); err != nil {
		return false, err
	}
	if problem := self.prePullProblem(); problem != "" {
		sampled := log.Logger.Sample(stuckPrePullSampler)
		sampled.Warn().Str("node", nodeName).Str("problem", problem).Msg("Image pre-pull is stuck; every rollout waits for it")
	}
	if !self.isPending {
		return false, nil
	}
	fleet, err := c.listRollouts()
	if err != nil {
		return false, err
	}
	if reason := rolloutHoldReason(fleet, self, c.config.Deploy.CaddyImage); reason != "" {
		log.Debug().Str("node", nodeName).Str("reason", reason).Msg("Holding Caddy rollout")
		return true, nil
	}
	log.Info().Str("node", nodeName).Msg("Rolling out Caddy")
	return false, nil
}

func rolloutHoldReason(fleet []nodeRollout, self nodeRollout, image string) string {
	firstPending, unavailable := "", ""
	for _, node := range fleet {
		if node.needsPrePull && !node.hasPrePulled(image) {
			return "waiting for the image pre-pull on node " + node.name
		}
		if node.name != self.name && !node.isAvailable {
			unavailable = node.name
		}
		if node.isPending && (firstPending == "" || node.name < firstPending) {
			firstPending = node.name
		}
	}
	switch {
	case !self.isAvailable:
		return ""
	case unavailable != "":
		return "waiting for node " + unavailable + " to become available"
	case firstPending != self.name:
		return "waiting for node " + firstPending + " to roll out first"
	}
	return ""
}

func (r nodeRollout) prePullProblem() string {
	if !r.needsPrePull || r.prePull == nil || caddy.IsImagePrePulled(r.prePull) {
		return ""
	}
	return caddy.PrePullProblem(r.prePull)
}

func (r nodeRollout) hasPrePulled(image string) bool {
	return r.prePull != nil && caddy.PrePullPodImage(r.prePull) == image && caddy.IsImagePrePulled(r.prePull)
}

func (c *Controller) listRollouts() ([]nodeRollout, error) {
	nodes, err := c.nodeLister.List(c.nodeSelector)
	if err != nil {
		return nil, err
	}
	fleet := make([]nodeRollout, 0, len(nodes))
	for _, node := range nodes {
		fleet = append(fleet, c.nodeRollout(node.Name))
	}
	return fleet, nil
}

func (c *Controller) nodeRollout(nodeName string) nodeRollout {
	opts := c.config.Deploy
	rollout := nodeRollout{name: nodeName, isAvailable: true}
	if prePull, err := c.podLister.Pods(c.config.Namespace).Get(caddy.PrePullPodName(nodeName)); err == nil {
		rollout.prePull = prePull
	}
	deployment, err := c.deployLister.Deployments(c.config.Namespace).Get(caddy.DeploymentName(nodeName))
	if err != nil {
		return rollout
	}
	rollout.isPending = caddy.AppliedTemplateHash(deployment) != caddy.TemplateHash(opts, nodeName)
	rollout.isAvailable = isRolledOut(deployment)
	rollout.needsPrePull = rollout.isPending && opts.PrePullImage && caddyImageOf(deployment) != opts.CaddyImage
	return rollout
}

func (c *Controller) syncPrePullPod(ctx context.Context, rollout nodeRollout, now time.Time) error {
	opts := c.config.Deploy
	pod := rollout.prePull
	isKept := pod != nil && rollout.needsPrePull && caddy.PrePullPodImage(pod) == opts.CaddyImage && !isRetryablePrePull(pod, now)
	switch {
	case pod != nil && !isKept:
		return caddy.DeletePrePullPod(ctx, opts, pod)
	case pod == nil && rollout.needsPrePull:
		return caddy.CreatePrePullPod(ctx, opts, rollout.name)
	}
	return nil
}

func isRetryablePrePull(pod *corev1.Pod, now time.Time) bool {
	return pod.Status.Phase == corev1.PodFailed && !caddy.IsImagePrePulled(pod) &&
		now.Sub(pod.CreationTimestamp.Time) >= prePullRetryInterval
}

func isRolledOut(deployment *appsv1.Deployment) bool {
	replicas := int32(1)
	if deployment.Spec.Replicas != nil {
		replicas = *deployment.Spec.Replicas
	}
	status := deployment.Status
	return status.ObservedGeneration >= deployment.Generation &&
		status.Replicas == replicas && status.UpdatedReplicas == replicas && status.AvailableReplicas == replicas
}

func caddyImageOf(deployment *appsv1.Deployment) string {
	if len(deployment.Spec.Template.Spec.Containers) == 0 {
		return ""
	}
	return deployment.Spec.Template.Spec.Containers[0].Image
}
