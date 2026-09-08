package caddy

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rs/zerolog"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"git.horse/vapronva/ckic/pkg/constants"
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
		{corev1.PodRunning, corev1.ContainerState{}, true, false},
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
		for _, isDeleting := range []bool{false, true} {
			if isDeleting {
				pod.DeletionTimestamp = &metav1.Time{}
			}
			pulled, err := prePullImagePresent(pod)
			wantFailed := test.failed || isDeleting && !test.pulled
			if pulled != test.pulled || (err != nil) != wantFailed {
				t.Errorf("phase=%s state=%+v deleting=%v: got %v, %v; want pulled=%v failed=%v", test.phase, test.state, isDeleting, pulled, err, test.pulled, wantFailed)
			}
		}
	}
}

func TestPrePullCleanupPreservesReplacement(t *testing.T) {
	for _, mode := range []string{"after pull", "reaper"} {
		t.Run(mode, func(t *testing.T) {
			pod := &corev1.Pod{
				Name: prePullPodName(testNode), Namespace: testNS, UID: "original",
				Labels: managedLabels(testNode), Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
			}
			pod.Labels[constants.LabelType] = constants.LabelTypeImagePrePull
			client := fake.NewClientset(pod)
			client.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, pod.DeepCopy(), client.Tracker().Add(pod)
			})
			deletes := 0
			client.PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				deletes++
				if mode == "after pull" && deletes == 1 {
					return false, nil, nil
				}
				replacement := pod.DeepCopy()
				replacement.UID = "replacement"
				if err := client.Tracker().Update(action.GetResource(), replacement, testNS); err != nil {
					t.Fatal(err)
				}
				options := action.(clienttesting.DeleteAction).GetDeleteOptions()
				if options.Preconditions != nil && options.Preconditions.UID != nil && *options.Preconditions.UID != replacement.UID {
					return true, nil, apierrors.NewConflict(corev1.Resource("pods"), pod.Name, errors.New("UID precondition failed"))
				}
				return false, nil, nil
			})
			if mode == "after pull" {
				opts := DeployOptions{Clientset: client, Namespace: testNS, CaddyImage: testImage}
				if err := prePullImage(t.Context(), opts, testNode, zerolog.Nop()); err != nil {
					t.Fatal(err)
				}
			} else {
				ReapPrePullPods(t.Context(), client, testNS, zerolog.Nop())
			}
			replacement, err := client.CoreV1().Pods(testNS).Get(t.Context(), pod.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("replacement pod was deleted by stale cleanup: %v", err)
			}
			if replacement.UID != "replacement" {
				t.Fatalf("replacement pod UID = %q", replacement.UID)
			}
		})
	}
}
