package caddy

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	prePullContainerName = "prepull"
	prePullRunAsID       = int64(1000)
)

func CreatePrePullPod(ctx context.Context, opts DeployOptions, nodeName string) error {
	if _, err := opts.Clientset.CoreV1().Pods(opts.Namespace).
		Apply(ctx, prePullPodApplyConfig(opts, nodeName), applyOptions()); err != nil {
		return fmt.Errorf("failed to create pre-pull pod for node %s: %w", nodeName, err)
	}
	log.Info().Str("node", nodeName).Str("image", opts.CaddyImage).Msg("Pre-pulling Caddy image on node")
	return nil
}

func PrePullPodImage(pod *corev1.Pod) string {
	for _, container := range pod.Spec.Containers {
		if container.Name == prePullContainerName {
			return container.Image
		}
	}
	return ""
}

func DeletePrePullPod(ctx context.Context, opts DeployOptions, pod *corev1.Pod) error {
	return deletePrePullPod(ctx, opts.Clientset, opts.Namespace, pod.Name, &pod.UID)
}

func prePullPodApplyConfig(opts DeployOptions, nodeName string) *corev1ac.PodApplyConfiguration {
	return corev1ac.Pod(PrePullPodName(nodeName), opts.Namespace).
		WithLabels(map[string]string{
			labelCaddyManaged: labelManagedValue,
			labelType:         labelTypePrePull,
			labelInstance:     nodeName,
		}).
		WithSpec(corev1ac.PodSpec().
			WithAffinity(nodeNameAffinity(nodeName)).
			WithRestartPolicy(corev1.RestartPolicyNever).
			WithAutomountServiceAccountToken(false).
			WithSecurityContext(corev1ac.PodSecurityContext().
				WithRunAsNonRoot(true).
				WithRunAsUser(prePullRunAsID).
				WithRunAsGroup(prePullRunAsID).
				WithSeccompProfile(corev1ac.SeccompProfile().WithType(corev1.SeccompProfileTypeRuntimeDefault))).
			WithContainers(corev1ac.Container().
				WithName(prePullContainerName).
				WithImage(opts.CaddyImage).
				WithImagePullPolicy(opts.ImagePullPolicy).
				WithCommand(caddyBinary, "version").
				WithSecurityContext(corev1ac.SecurityContext().
					WithAllowPrivilegeEscalation(false).
					WithRunAsNonRoot(true).
					WithRunAsUser(prePullRunAsID).
					WithRunAsGroup(prePullRunAsID).
					WithCapabilities(corev1ac.Capabilities().WithAdd("NET_BIND_SERVICE").WithDrop("ALL")).
					WithSeccompProfile(corev1ac.SeccompProfile().WithType(corev1.SeccompProfileTypeRuntimeDefault)))))
}

func IsImagePrePulled(pod *corev1.Pod) bool {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != prePullContainerName {
			continue
		}
		if status.ContainerID != "" && (status.State.Running != nil || status.State.Terminated != nil) {
			return true
		}
		if waiting := status.State.Waiting; waiting != nil {
			switch waiting.Reason {
			case "CreateContainerConfigError",
				"CreateContainerError",
				"PreCreateHookError",
				"PreStartHookError",
				"PostStartHookError",
				"RunContainerError",
				"CrashLoopBackOff":
				return true
			}
		}
	}
	return false
}

func PrePullProblem(pod *corev1.Pod) string {
	if pod.Status.Phase == corev1.PodFailed {
		return "pod failed: " + pod.Status.Reason
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse {
			return condition.Reason + ": " + condition.Message
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if waiting := status.State.Waiting; status.Name == prePullContainerName && waiting != nil && waiting.Reason != "ContainerCreating" {
			return waiting.Reason + ": " + waiting.Message
		}
	}
	return ""
}

func deletePrePullPod(ctx context.Context, clientset kubernetes.Interface, namespace, podName string, uid *types.UID) error {
	err := clientset.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: new(int64(0)),
		Preconditions:      &metav1.Preconditions{UID: uid},
	})
	if err == nil || apierrors.IsNotFound(err) || uid != nil && apierrors.IsConflict(err) {
		return nil
	}
	return fmt.Errorf("failed to delete pre-pull pod %s: %w", podName, err)
}
