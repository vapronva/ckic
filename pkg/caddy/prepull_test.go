package caddy

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestPrePullImagePresent(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		phase          corev1.PodPhase
		state          corev1.ContainerState
		pulled, failed bool
	}{
		{corev1.PodPending, corev1.ContainerState{}, false, false},
		{corev1.PodSucceeded, corev1.ContainerState{}, true, false},
		{corev1.PodFailed, corev1.ContainerState{}, false, true},
		{corev1.PodPending, corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, true, false},
		{corev1.PodFailed, corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}, true, false},
		{corev1.PodPending, corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CreateContainerConfigError"}}, true, false},
		{corev1.PodPending, corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}, false, true},
		{corev1.PodPending, corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}, false, false},
	} {
		pod := &corev1.Pod{Status: corev1.PodStatus{
			Phase: test.phase,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "sidecar", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				{Name: "prepull", State: test.state},
			},
		}}
		pulled, err := prePullImagePresent(pod)
		if pulled != test.pulled || (err != nil) != test.failed {
			t.Fatalf("phase=%s state=%+v: got %v, %v; want pulled=%v failed=%v", test.phase, test.state, pulled, err, test.pulled, test.failed)
		}
	}
}
