package caddy

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestIsImagePrePulled(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		state       corev1.ContainerState
		containerID string
		isPulled    bool
	}{
		{corev1.ContainerState{}, "", false},
		{corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, "containerd://1", true},
		{corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}, "containerd://1", true},
		{corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "ContainerStatusUnknown"}}, "", false},
		{corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CreateContainerConfigError"}}, "", true},
		{corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}, "", false},
		{corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}, "", false},
	} {
		pod := &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "sidecar", ContainerID: "containerd://0", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
			{Name: "prepull", ContainerID: test.containerID, State: test.state},
		}}}
		if got := IsImagePrePulled(pod); got != test.isPulled {
			t.Errorf("state=%+v containerID=%q: pulled = %v, want %v", test.state, test.containerID, got, test.isPulled)
		}
	}
}

func TestDeletePrePullPodPreservesReplacement(t *testing.T) {
	t.Parallel()
	replacement := &corev1.Pod{Name: PrePullPodName(testNode), Namespace: testNS, UID: "replacement"}
	client := fake.NewClientset(replacement)
	client.PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		preconditions := action.(clienttesting.DeleteAction).GetDeleteOptions().Preconditions
		if preconditions == nil || preconditions.UID == nil || *preconditions.UID != replacement.UID {
			return true, nil, apierrors.NewConflict(corev1.Resource("pods"), replacement.Name, errors.New("UID precondition failed"))
		}
		return false, nil, nil
	})
	stale := replacement.DeepCopy()
	stale.UID = "original"
	if err := DeletePrePullPod(t.Context(), DeployOptions{Clientset: client, Namespace: testNS}, stale); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods(testNS).Get(t.Context(), replacement.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("replacement pre-pull pod was deleted by a stale cleanup: %v", err)
	}
}
