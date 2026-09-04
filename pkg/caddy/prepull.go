package caddy

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/constants"
)

const (
	imagePrePullTimeout   = 3 * time.Minute
	imagePrePullPollDelay = 2 * time.Second
	prePullCleanupTimeout = 30 * time.Second
	prePullContainerName  = "prepull"
	prePullRunAsID        = int64(1000)
)

func prePullImage(ctx context.Context, opts DeployOptions, nodeName string, logger zerolog.Logger) error {
	podName := prePullPodName(nodeName)
	logger = logger.With().Str("prepullPod", podName).Str("image", opts.CaddyImage).Logger()
	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), prePullCleanupTimeout)
		defer cancel()
		if err := deletePrePullPod(cleanupCtx, opts.Clientset, opts.Namespace, podName); err != nil {
			logger.Warn().Err(err).Msg("Failed to delete pre-pull pod")
		}
	}
	cleanup()
	if _, err := opts.Clientset.CoreV1().Pods(opts.Namespace).
		Apply(ctx, prePullPodApplyConfig(opts, nodeName), applyOptions()); err != nil {
		return fmt.Errorf("failed to create pre-pull pod: %w", err)
	}
	defer cleanup()
	logger.Info().Msg("Pre-pulling Caddy image on node")
	if err := waitForImagePulled(ctx, opts.Clientset, opts.Namespace, podName); err != nil {
		return err
	}
	logger.Info().Msg("Caddy image present on node")
	return nil
}

func prePullPodApplyConfig(opts DeployOptions, nodeName string) *corev1ac.PodApplyConfiguration {
	return corev1ac.Pod(prePullPodName(nodeName), opts.Namespace).
		WithLabels(map[string]string{
			constants.LabelCaddyManaged: constants.LabelManagedValue,
			constants.LabelType:         constants.LabelTypeImagePrePull,
			constants.LabelInstance:     nodeName,
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
					WithCapabilities(corev1ac.Capabilities().WithDrop("ALL")).
					WithSeccompProfile(corev1ac.SeccompProfile().WithType(corev1.SeccompProfileTypeRuntimeDefault)))))
}

func waitForImagePulled(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) error {
	return wait.PollUntilContextTimeout(ctx, imagePrePullPollDelay, imagePrePullTimeout, true,
		func(ctx context.Context) (bool, error) {
			pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to get pre-pull pod: %w", err)
			}
			return prePullImagePresent(pod)
		})
}

func prePullImagePresent(pod *corev1.Pod) (bool, error) {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != prePullContainerName {
			continue
		}
		if status.State.Running != nil || status.State.Terminated != nil {
			return true, nil
		}
		waiting := status.State.Waiting
		if waiting == nil {
			continue
		}
		switch waiting.Reason {
		case "ImagePullBackOff",
			"ErrImagePull",
			"ErrImageNeverPull",
			"InvalidImageName",
			"ImageInspectError",
			"RegistryUnavailable",
			"SignatureValidationFailed":
			return false, fmt.Errorf("image pull failed (%s): %s", waiting.Reason, waiting.Message)
		case "CreateContainerConfigError",
			"CreateContainerError",
			"PreCreateHookError",
			"PreStartHookError",
			"PostStartHookError",
			"RunContainerError",
			"CrashLoopBackOff":
			return true, nil
		}
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodRunning:
		return true, nil
	case corev1.PodFailed:
		return false, fmt.Errorf("pre-pull pod failed before image pull (reason=%s): %s", pod.Status.Reason, pod.Status.Message)
	case corev1.PodPending, corev1.PodUnknown:
	}
	return false, nil
}

func deletePrePullPod(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) error {
	gracePeriod := int64(0)
	err := clientset.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod})
	if err == nil || apierrors.IsNotFound(err) {
		return nil
	}
	return fmt.Errorf("failed to delete pre-pull pod %s: %w", podName, err)
}

func ReapPrePullPods(ctx context.Context, clientset kubernetes.Interface, namespace string, logger zerolog.Logger) {
	ctx, cancel := context.WithTimeout(ctx, prePullCleanupTimeout)
	defer cancel()
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			constants.LabelCaddyManaged: constants.LabelManagedValue,
			constants.LabelType:         constants.LabelTypeImagePrePull,
		}).String(),
	})
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to list leftover pre-pull pods for cleanup")
		return
	}
	for i := range pods.Items {
		if err := deletePrePullPod(ctx, clientset, namespace, pods.Items[i].Name); err != nil {
			logger.Warn().Err(err).Msg("Failed to delete leftover pre-pull pod")
		}
	}
}
