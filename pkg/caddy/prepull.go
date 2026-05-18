package caddy

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/constants"
)

const (
	imagePrePullTimeout   = 3 * time.Minute
	imagePrePullPollDelay = 2 * time.Second
	prePullContainerName  = "prepull"
)

func prePullImage(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace, nodeName, image string,
	pullPolicy corev1.PullPolicy,
	logger zerolog.Logger,
) error {
	podName := prePullPodName(nodeName)
	logger = logger.With().
		Str("prepullPod", podName).
		Str("image", image).
		Logger()
	cleanupCtx := context.WithoutCancel(ctx)
	deletePrePullPod(cleanupCtx, clientset, namespace, podName)
	pod := prePullPodSpec(podName, namespace, nodeName, image, pullPolicy)
	if _, err := clientset.CoreV1().
		Pods(namespace).
		Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create pre-pull pod: %w", err)
	}
	defer deletePrePullPod(cleanupCtx, clientset, namespace, podName)
	logger.Info().Msg("Pre-pulling Caddy image on node")
	if err := waitForImagePulled(ctx, clientset, namespace, podName); err != nil {
		return err
	}
	logger.Info().Msg("Caddy image present on node")
	return nil
}

func prePullPodName(nodeName string) string {
	return "caddy-prepull-" + nodeName
}

func prePullPodSpec(
	podName, namespace, nodeName, image string,
	pullPolicy corev1.PullPolicy,
) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				constants.LabelCaddyManaged: constants.LabelManagedValue,
				constants.LabelType:         constants.LabelTypeImagePrePull,
				constants.LabelInstance:     nodeName,
			},
		},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{
				constants.HostLabelHostname: nodeName,
			},
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: new(false),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: new(true),
				RunAsUser:    new(caddyRunAsUser),
				RunAsGroup:   new(caddyRunAsGroup),
				SeccompProfile: &corev1.SeccompProfile{
					Type: corev1.SeccompProfileTypeRuntimeDefault,
				},
			},
			Containers: []corev1.Container{
				{
					Name:            prePullContainerName,
					Image:           image,
					ImagePullPolicy: pullPolicy,
					Command:         []string{caddyBinary, "version"},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: new(false),
						RunAsNonRoot:             new(true),
						RunAsUser:                new(caddyRunAsUser),
						RunAsGroup:               new(caddyRunAsGroup),
						Capabilities: &corev1.Capabilities{
							Drop: []corev1.Capability{caddyCapabilityDrop},
						},
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
				},
			},
		},
	}
}

func waitForImagePulled(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace, podName string,
) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, imagePrePullTimeout)
	defer cancel()
	for {
		pod, err := clientset.CoreV1().
			Pods(namespace).
			Get(deadlineCtx, podName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get pre-pull pod: %w", err)
		}
		pulled, pullErr := prePullImagePresent(pod)
		if pullErr != nil {
			return pullErr
		}
		if pulled {
			return nil
		}
		timer := time.NewTimer(imagePrePullPollDelay)
		select {
		case <-deadlineCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf(
				"timed out waiting for image pre-pull: %w",
				deadlineCtx.Err(),
			)
		case <-timer.C:
		}
	}
}

func prePullImagePresent(pod *corev1.Pod) (bool, error) {
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodRunning, corev1.PodFailed:
		return true, nil
	case corev1.PodPending, corev1.PodUnknown:
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Running != nil || status.State.Terminated != nil {
			return true, nil
		}
		waiting := status.State.Waiting
		if waiting == nil {
			continue
		}
		switch waiting.Reason {
		case "ImagePullBackOff", "ErrImagePull", "InvalidImageName":
			return false, fmt.Errorf(
				"image pull failed (%s): %s",
				waiting.Reason,
				waiting.Message,
			)
		}
	}
	return false, nil
}

func deletePrePullPod(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace, podName string,
) {
	gracePeriod := int64(0)
	err := clientset.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		log.Warn().
			Err(err).
			Str("prepullPod", podName).
			Msg("Failed to delete pre-pull pod")
	}
}
