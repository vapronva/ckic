package controller

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"git.horse/vapronva/ckic/pkg/caddy"
)

var stuckRolloutSampler = &zerolog.BurstSampler{Burst: 1, Period: time.Minute}

type nodeRollout struct {
	name         string
	isInService  bool
	isPending    bool
	isAvailable  bool
	needsPrePull bool
	prePull      *corev1.Pod
	deployment   *appsv1.Deployment
}

func (c *Controller) deployNode(ctx context.Context, node *corev1.Node) (caddy.Pod, error) {
	if err := c.applyCaddy(ctx, node); err != nil {
		return caddy.Pod{}, err
	}
	return caddy.FindActivePod(ctx, c.config.Deploy, node.Name)
}

func (c *Controller) applyCaddy(ctx context.Context, node *corev1.Node) error {
	isHeld, err := c.syncRollout(ctx, node)
	if err != nil {
		return err
	}
	opts, externalIPs := c.config.Deploy, c.config.ExternalEndpoints[node.Name]
	switch {
	case isHeld && opts.EnableCiliumLB:
		return caddy.ApplyLoadBalancerService(ctx, opts, node.Name, externalIPs)
	case isHeld:
		return nil
	}
	if err := caddy.EnsureLoadBalancerService(ctx, opts, node.Name, externalIPs); err != nil {
		return err
	}
	return caddy.DeleteStaleDeployments(ctx, opts, node.Name)
}

func (c *Controller) syncRollout(ctx context.Context, node *corev1.Node) (bool, error) {
	self := c.nodeRollout(node, c.cachedDeployment(node.Name))
	if err := c.syncPrePullPod(ctx, self, time.Now()); err != nil {
		return false, err
	}
	switch {
	case !self.isPending:
		return false, caddy.ApplyDeployment(ctx, c.config.Deploy, node.Name)
	case !self.isInService:
		log.Debug().Str("node", node.Name).Msg("Holding Caddy rollout until the node is schedulable and ready")
		return true, nil
	}
	return c.admitRollout(ctx, node.Name)
}

func (c *Controller) admitRollout(ctx context.Context, nodeName string) (bool, error) {
	c.rolloutMu.Lock()
	defer c.rolloutMu.Unlock()
	fleet, err := c.listRollouts(ctx)
	if err != nil {
		return false, err
	}
	image := c.config.Deploy.CaddyImage
	reason, blocker := rolloutHold(fleet, nodeName, image)
	if reason != "" {
		logRolloutHold(nodeName, reason, c.blockerProblem(blocker, image))
		c.queue.AddAfter(nodeName, requeueInterval)
		return true, nil
	}
	log.Info().Str("node", nodeName).Msg("Rolling out Caddy")
	return false, caddy.ApplyDeployment(ctx, c.config.Deploy, nodeName)
}

func logRolloutHold(nodeName, reason, problem string) {
	if problem == "" {
		log.Debug().Str("node", nodeName).Str("reason", reason).Msg("Holding Caddy rollout")
		return
	}
	sampled := log.Logger.Sample(stuckRolloutSampler)
	sampled.Warn().Str("node", nodeName).Str("reason", reason).Str("status", problem).
		Msg("Caddy rollout is stuck; fix or cordon the blocking node")
}

func rolloutHold(fleet []nodeRollout, selfName, image string) (string, *nodeRollout) {
	var self, unavailable *nodeRollout
	firstPending := ""
	for _, node := range fleet {
		if node.needsPrePull && !node.hasPrePulled(image) {
			return "waiting for the image pre-pull on node " + node.name, &node
		}
		switch {
		case node.name == selfName:
			self = &node
		case !node.isAvailable:
			unavailable = &node
		}
		if node.isPending && (firstPending == "" || node.name < firstPending) {
			firstPending = node.name
		}
	}
	switch {
	case self == nil:
		return "node is cordoned or not ready", nil
	case !self.isPending || !self.isAvailable:
		return "", nil
	case unavailable != nil:
		return "waiting for node " + unavailable.name + " to become available", unavailable
	case firstPending != self.name:
		return "waiting for node " + firstPending + " to roll out first", nil
	}
	return "", nil
}

func (c *Controller) blockerProblem(blocker *nodeRollout, image string) string {
	switch {
	case blocker == nil:
		return ""
	case blocker.needsPrePull && !blocker.hasPrePulled(image):
		return blocker.prePullProblem(image)
	case !blocker.isAvailable:
		return c.caddyProblem(*blocker)
	}
	return ""
}

func (r nodeRollout) prePullProblem(image string) string {
	if r.prePull == nil || caddy.PrePullPodImage(r.prePull) != image {
		return ""
	}
	return caddy.PodProblem(r.prePull)
}

func (r nodeRollout) hasPrePulled(image string) bool {
	return r.prePull != nil && caddy.PrePullPodImage(r.prePull) == image && caddy.IsImagePrePulled(r.prePull)
}

func (c *Controller) caddyProblem(rollout nodeRollout) string {
	pods, err := c.podLister.Pods(c.config.Namespace).List(labels.SelectorFromSet(caddy.ManagedLabels(rollout.name)))
	if err != nil {
		return ""
	}
	for _, pod := range pods {
		if !caddy.IsActivePod(pod) {
			continue
		}
		if problem := caddy.PodProblem(pod); problem != "" {
			return problem
		}
	}
	return caddy.DeploymentProblem(rollout.deployment, time.Now())
}

func (c *Controller) listRollouts(ctx context.Context) ([]nodeRollout, error) {
	nodes, err := c.nodeLister.List(c.nodeSelector)
	if err != nil {
		return nil, err
	}
	deployments, err := caddy.ListDeployments(ctx, c.config.Deploy)
	if err != nil {
		return nil, err
	}
	fleet := make([]nodeRollout, 0, len(nodes))
	for _, node := range nodes {
		if isInService(node) {
			fleet = append(fleet, c.nodeRollout(node, deployments[caddy.DeploymentName(node.Name)]))
		}
	}
	return fleet, nil
}

func (c *Controller) cachedDeployment(nodeName string) *appsv1.Deployment {
	deployment, err := c.deployLister.Deployments(c.config.Namespace).Get(caddy.DeploymentName(nodeName))
	if err != nil {
		return nil
	}
	return deployment
}

func (c *Controller) nodeRollout(node *corev1.Node, deployment *appsv1.Deployment) nodeRollout {
	opts := c.config.Deploy
	rollout := nodeRollout{name: node.Name, isInService: isInService(node), isAvailable: true, deployment: deployment}
	if prePull, err := c.podLister.Pods(c.config.Namespace).Get(caddy.PrePullPodName(node.Name)); err == nil {
		rollout.prePull = prePull
	}
	if deployment == nil {
		return rollout
	}
	rollout.isPending = caddy.AppliedTemplateHash(deployment) != caddy.TemplateHash(opts, node.Name)
	rollout.isAvailable = isRolledOut(deployment)
	rollout.needsPrePull = rollout.isPending && rollout.isInService && opts.PrePullImage && caddyImageOf(deployment) != opts.CaddyImage
	return rollout
}

func isInService(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
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
