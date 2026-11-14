package caddy

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	prePullContainerName = "prepull"
	prePullRunAsID       = int64(1000)
)

func CreatePrePullPod(ctx context.Context, opts DeployOptions, nodeName string) error {
	_, err := opts.Clientset.CoreV1().Pods(opts.Namespace).Create(ctx, prePullPod(opts, nodeName), metav1.CreateOptions{FieldManager: fieldManager})
	if apierrors.IsAlreadyExists(err) {
		return checkPrePullPodOwned(ctx, opts, nodeName)
	}
	if err != nil {
		return fmt.Errorf("failed to create pre-pull pod for node %s: %w", nodeName, err)
	}
	log.Info().Str("node", nodeName).Str("image", opts.CaddyImage).Msg("Pre-pulling Caddy image on node")
	return nil
}

func checkPrePullPodOwned(ctx context.Context, opts DeployOptions, nodeName string) error {
	name := PrePullPodName(nodeName)
	pod, err := opts.Clientset.CoreV1().Pods(opts.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to read existing pre-pull pod %s: %w", name, err)
	}
	if !IsPrePullPod(pod, nodeName) {
		return fmt.Errorf("pod %s is not a CKIC pre-pull pod; rename or delete it to let node %s roll out", name, nodeName)
	}
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

func IsPrePullPod(pod *corev1.Pod, nodeName string) bool {
	return pod.Name == PrePullPodName(nodeName) && labels.SelectorFromSet(prePullLabels(nodeName)).Matches(labels.Set(pod.Labels))
}

func DeletePrePullPod(ctx context.Context, opts DeployOptions, pod *corev1.Pod) error {
	err := opts.Clientset.CoreV1().Pods(opts.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
		GracePeriodSeconds: new(int64(0)),
		Preconditions:      &metav1.Preconditions{UID: &pod.UID},
	})
	if err == nil || apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return fmt.Errorf("failed to delete pre-pull pod %s: %w", pod.Name, err)
}

func deletePrePullPods(ctx context.Context, opts DeployOptions, nodeName string) error {
	selector := labels.SelectorFromSet(prePullLabels(nodeName)).String()
	owned, err := opts.Clientset.CoreV1().Pods(opts.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("failed to list pre-pull pods for node %s: %w", nodeName, err)
	}
	for idx := range owned.Items {
		if err := DeletePrePullPod(ctx, opts, &owned.Items[idx]); err != nil {
			return err
		}
	}
	return nil
}

func prePullPod(opts DeployOptions, nodeName string) *corev1.Pod {
	nodeRequirement := corev1.NodeSelectorRequirement{Key: metav1.ObjectNameField, Operator: corev1.NodeSelectorOpIn, Values: []string{nodeName}}
	return &corev1.Pod{
		Name:      PrePullPodName(nodeName),
		Namespace: opts.Namespace,
		Labels:    prePullLabels(nodeName),
		Spec: corev1.PodSpec{
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{nodeRequirement}}},
				},
			}},
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: new(false),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   new(true),
				RunAsUser:      new(prePullRunAsID),
				RunAsGroup:     new(prePullRunAsID),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:            prePullContainerName,
				Image:           opts.CaddyImage,
				ImagePullPolicy: opts.ImagePullPolicy,
				Command:         []string{caddyBinary, "version"},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: new(false),
					Capabilities:             &corev1.Capabilities{Add: []corev1.Capability{"NET_BIND_SERVICE"}, Drop: []corev1.Capability{"ALL"}},
				},
			}},
		},
	}
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
