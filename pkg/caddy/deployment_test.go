package caddy

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/validate/content"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	testNS    = "caddy-system"
	testNode  = "node1"
	testImage = "example.com/caddy:test"
)

func runningCaddyPod() *corev1.Pod {
	return &corev1.Pod{
		Name:      "caddy-" + testNode + "-abc",
		Namespace: testNS,
		Labels:    ManagedLabels(testNode),
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "192.0.2.1",
			ContainerStatuses: []corev1.ContainerStatus{{Name: "caddy", ContainerID: "container-1", Started: new(true)}},
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func TestApplyCaddyModeTransitions(t *testing.T) {
	t.Parallel()
	unrelated := runningCaddyPod()
	unrelated.Name = "unrelated-newer-pod"
	unrelated.CreationTimestamp = metav1.NewTime(time.Now().Add(time.Hour))
	delete(unrelated.Labels, labelCaddyManaged)
	client := fake.NewClientset(
		runningCaddyPod(), unrelated,
		&corev1.Service{Name: "caddy-node1", Namespace: testNS, Labels: ManagedLabels(testNode)},
		&corev1.Service{Name: "caddy-node1-lb", Namespace: testNS, Labels: ManagedLabels(testNode)},
		&corev1.Service{Name: "caddy-loadbalancer", Namespace: testNS},
	)
	opts := DeployOptions{Clientset: client, Namespace: testNS, CaddyImage: testImage, ImagePullPolicy: corev1.PullIfNotPresent, BootConfigMapName: "caddy-config"}
	for _, mode := range []struct {
		name                string
		cilium, hostNetwork bool
		services            int
	}{
		{"cilium", true, false, 1},
		{"none", false, false, 0},
		{"hostnetwork", false, true, 0},
	} {
		t.Run(mode.name, func(t *testing.T) {
			opts.EnableCiliumLB = mode.cilium
			opts.UseHostNetwork = mode.hostNetwork
			for range 2 {
				if err := errors.Join(
					ApplyDeployment(t.Context(), opts, testNode),
					EnsureLoadBalancerService(t.Context(), opts, testNode, []string{"203.0.113.5"}),
					DeleteStaleDeployments(t.Context(), opts, testNode),
				); err != nil {
					t.Fatal(err)
				}
				pod, err := FindStartedPod(t.Context(), opts, testNode)
				if err != nil {
					t.Fatal(err)
				}
				if pod != (Pod{Name: "caddy-node1-abc", IP: "192.0.2.1", ContainerID: "container-1"}) {
					t.Fatalf("pod was not resolved: %+v", pod)
				}
			}
			deployment, err := client.AppsV1().Deployments(testNS).Get(t.Context(), "caddy-node1", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			podSpec := deployment.Spec.Template.Spec
			affinity := podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields[0]
			if affinity.Key != metav1.ObjectNameField || !reflect.DeepEqual(affinity.Values, []string{testNode}) {
				t.Fatalf("node affinity = %+v", affinity)
			}
			if podSpec.HostNetwork != mode.hostNetwork {
				t.Fatalf("host network = %v", podSpec.HostNetwork)
			}
			if mode.hostNetwork {
				if deployment.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
					t.Fatalf("host networking must use Recreate: %+v", deployment.Spec.Strategy)
				}
				for _, port := range podSpec.Containers[0].Ports {
					if port.Name != "admin" && port.HostPort != port.ContainerPort {
						t.Fatalf("host port differs from container port: %+v", port)
					}
				}
			}
			services, err := client.CoreV1().Services(testNS).List(t.Context(), metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(services.Items) != mode.services+1 {
				t.Fatalf("services = %d, want %d", len(services.Items), mode.services+1)
			}
			for _, service := range services.Items {
				if service.Name == "caddy-loadbalancer" {
					continue
				}
				if service.Name != "caddy-node1-loadbalancer" || !reflect.DeepEqual(service.Spec.Selector, deployment.Spec.Template.Labels) {
					t.Fatalf("unexpected service %s selecting %+v", service.Name, service.Spec.Selector)
				}
				if service.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal || !reflect.DeepEqual(service.Spec.ExternalIPs, []string{"203.0.113.5"}) {
					t.Fatalf("per-node LoadBalancer spec = %+v", service.Spec)
				}
			}
		})
	}
}

func TestNodeDerivedNames(t *testing.T) {
	t.Parallel()
	const dotted = "worker.example.com"
	if DeploymentName(dotted) != "caddy-"+dotted || PrePullPodName(dotted) != "caddy-prepull-"+dotted {
		t.Fatal("Deployment and pre-pull identities must preserve the node name")
	}
	if loadBalancerServiceName(dotted) == loadBalancerServiceName("worker-example-com") {
		t.Fatal("dotted and hyphenated node names collide")
	}
	longest := strings.Repeat("n", content.LabelValueMaxLength)
	for _, node := range []string{dotted, longest} {
		if errs := validation.IsDNS1035Label(loadBalancerServiceName(node)); len(errs) != 0 {
			t.Fatalf("invalid Service name for %q: %v", node, errs)
		}
	}
	opts := DeployOptions{Clientset: fake.NewClientset(), Namespace: testNS}
	if err := ApplyDeployment(t.Context(), opts, longest); err != nil {
		t.Fatalf("longest label-value node name must be accepted: %v", err)
	}
	if err := ApplyDeployment(t.Context(), opts, longest+"n"); err == nil {
		t.Fatal("node names exceeding the instance label limit must be rejected")
	}
}

func TestNewestStartedPodSkipsUnstartedSurge(t *testing.T) {
	t.Parallel()
	now := time.Now()
	started := func(name string, created time.Time) corev1.Pod {
		pod := *runningCaddyPod()
		pod.Name, pod.CreationTimestamp = name, metav1.NewTime(created)
		return pod
	}
	surge := corev1.Pod{Name: "surge", CreationTimestamp: metav1.NewTime(now.Add(time.Minute)), Status: corev1.PodStatus{Phase: corev1.PodPending}}
	crashing := started("crashing", now.Add(time.Minute))
	crashing.Status.ContainerStatuses[0].Started = new(false)
	terminating := started("terminating", now.Add(time.Minute))
	terminating.DeletionTimestamp = &terminating.CreationTimestamp
	failed := started("failed", now.Add(time.Minute))
	failed.Status.Phase = corev1.PodFailed
	pods := []corev1.Pod{started("serving", now), surge, crashing, terminating, failed, started("older", now.Add(-time.Hour))}
	if pod := newestStartedPod(pods); pod == nil || pod.Name != "serving" {
		t.Fatalf("newestStartedPod = %v, want the serving pod", pod)
	}
	tie := []corev1.Pod{started("aaa", now), started("bbb", now)}
	if pod := newestStartedPod(tie); pod == nil || pod.Name != "bbb" {
		t.Fatalf("tie-break = %v, want bbb", pod)
	}
	if pod := newestStartedPod([]corev1.Pod{surge}); pod != nil {
		t.Fatalf("unstarted pod selected: %v", pod.Name)
	}
}

func TestDeploymentProblemIgnoresNormalRollouts(t *testing.T) {
	t.Parallel()
	now := time.Now()
	unavailableSince := func(age time.Duration, extra ...appsv1.DeploymentCondition) *appsv1.Deployment {
		deployment := &appsv1.Deployment{Generation: 2}
		deployment.Spec.ProgressDeadlineSeconds = new(int32(600))
		deployment.Status.ObservedGeneration = 2
		deployment.Status.Conditions = append(extra,
			appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "NewReplicaSetAvailable"},
			appsv1.DeploymentCondition{
				Type: appsv1.DeploymentAvailable, Status: corev1.ConditionFalse, Reason: "MinimumReplicasUnavailable",
				LastTransitionTime: metav1.NewTime(now.Add(-age)),
			})
		return deployment
	}
	for _, test := range []struct {
		name       string
		deployment *appsv1.Deployment
		isProblem  bool
	}{
		{"recreate or rollback in progress", unavailableSince(30 * time.Second), false},
		{"readiness lost past the progress deadline", unavailableSince(11 * time.Minute), true},
		{"replacement rejected", unavailableSince(time.Second, appsv1.DeploymentCondition{
			Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate",
		}), true},
	} {
		if got := DeploymentProblem(test.deployment, now) != ""; got != test.isProblem {
			t.Errorf("%s: problem = %v, want %v", test.name, got, test.isProblem)
		}
	}
}
