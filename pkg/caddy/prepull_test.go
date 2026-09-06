package caddy

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestWaitForImagePulled(t *testing.T) {
	denied := apierrors.NewUnauthorized("denied")
	for _, test := range []struct {
		name      string
		responses []error
		wantError error
		wantWait  time.Duration
	}{
		{"unavailable", []error{apierrors.NewServiceUnavailable("unavailable"), nil}, nil, imagePrePullPollDelay},
		{"throttled", []error{apierrors.NewTooManyRequests("throttled", 0), nil}, nil, imagePrePullPollDelay},
		{"http2 connection lost", []error{errors.New("http2: client connection lost"), nil}, nil, imagePrePullPollDelay},
		{"unauthorized", []error{denied}, denied, 0},
		{"timeout", []error{apierrors.NewServiceUnavailable("unavailable")}, context.DeadlineExceeded, imagePrePullTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := fake.NewClientset(&corev1.Pod{Name: "prepull", Namespace: testNS, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}})
				responses := test.responses
				client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
					err := responses[0]
					if len(responses) > 1 {
						responses = responses[1:]
					}
					return err != nil, nil, err
				})
				start := time.Now()
				err := waitForImagePulled(t.Context(), client, testNS, "prepull")
				if !errors.Is(err, test.wantError) {
					t.Fatalf("got %v, want %v", err, test.wantError)
				}
				if elapsed := time.Since(start); elapsed != test.wantWait {
					t.Fatalf("poll stopped after %s, want %s", elapsed, test.wantWait)
				}
			})
		})
	}
}

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
