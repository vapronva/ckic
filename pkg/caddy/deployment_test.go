package caddy

import (
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	"git.horse/vapronva/ckic/pkg/constants"
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
		Labels:    managedLabels(testNode),
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "192.0.2.1",
			ContainerStatuses: []corev1.ContainerStatus{{Name: "caddy", ContainerID: "container-1"}},
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func TestEnsureCaddyModeTransitions(t *testing.T) {
	t.Parallel()
	unrelated := runningCaddyPod()
	unrelated.Name = "unrelated-newer-pod"
	unrelated.CreationTimestamp = metav1.NewTime(time.Now().Add(time.Hour))
	delete(unrelated.Labels, constants.LabelCaddyManaged)
	client := fake.NewClientset(
		runningCaddyPod(), unrelated,
		&corev1.Service{Name: "caddy-node1", Namespace: testNS, Labels: managedLabels(testNode)},
		&corev1.Service{Name: "caddy-node1-lb", Namespace: testNS, Labels: managedLabels(testNode)},
		&corev1.Service{Name: "caddy-loadbalancer", Namespace: testNS},
	)
	opts := DeployOptions{Clientset: client, Namespace: testNS, CaddyImage: testImage, ImagePullPolicy: corev1.PullIfNotPresent, ConfigMapName: "caddy-config"}
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
				instance, err := EnsureCaddy(t.Context(), opts, testNode, []string{"203.0.113.5"})
				if err != nil {
					t.Fatal(err)
				}
				if instance.PodName != "caddy-node1-abc" || instance.PodIP != "192.0.2.1" || instance.ContainerID != "container-1" {
					t.Fatalf("pod was not resolved: %+v", instance)
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
	if DeploymentName(dotted) != "caddy-"+dotted || prePullPodName(dotted) != "caddy-prepull-"+dotted {
		t.Fatal("Deployment and pre-pull identities must preserve the node name")
	}
	if loadBalancerServiceName(dotted) == loadBalancerServiceName("worker-example-com") {
		t.Fatal("dotted and hyphenated node names collide")
	}
	for _, node := range []string{dotted, strings.Repeat("n", 63)} {
		if errs := validation.IsDNS1035Label(loadBalancerServiceName(node)); len(errs) != 0 {
			t.Fatalf("invalid Service name for %q: %v", node, errs)
		}
	}
	opts := DeployOptions{Clientset: fake.NewClientset(), Namespace: testNS}
	if _, err := EnsureCaddy(t.Context(), opts, strings.Repeat("n", 63), nil); err != nil {
		t.Fatalf("63-char node name must be accepted: %v", err)
	}
	if _, err := EnsureCaddy(t.Context(), opts, strings.Repeat("n", 64), nil); err == nil {
		t.Fatal("node names exceeding the instance label limit must be rejected")
	}
}

func TestEnsureCaddyPreservesFileMountOnUpgrade(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	opts := DeployOptions{Clientset: client, Namespace: testNS, CaddyImage: testImage, ConfigMapName: "caddy-config"}
	instance := &Instance{NodeName: testNode, Namespace: testNS, DeploymentName: DeploymentName(testNode)}
	legacy := deploymentApplyConfig(instance, opts)
	legacy.Spec.Template.Spec.Containers[0].VolumeMounts[0] = *corev1ac.VolumeMount().
		WithName(constants.VolumeNameCaddyConfig).WithMountPath("/etc/caddy/Caddyfile")
	deployments := client.AppsV1().Deployments(testNS)
	if _, err := deployments.Apply(t.Context(), legacy, metav1.ApplyOptions{FieldManager: "legacy"}); err != nil {
		t.Fatal(err)
	}
	legacy.Spec.Template.Spec.Containers[0].VolumeMounts[0].WithSubPath("Caddyfile")
	if _, err := deployments.Apply(t.Context(), legacy, applyOptions()); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsureCaddy(t.Context(), opts, testNode, nil); err != nil {
		t.Fatal(err)
	}
	deployment, err := deployments.Get(t.Context(), instance.DeploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mounts := deployment.Spec.Template.Spec.Containers[0].VolumeMounts
	if len(mounts) != 3 || mounts[0].MountPath != "/etc/caddy/Caddyfile" || mounts[0].SubPath != "Caddyfile" {
		t.Fatalf("upgrade changed the config file mount: %+v", mounts)
	}
}

func TestSelectNewestActivePod(t *testing.T) {
	t.Parallel()
	now := time.Now()
	older := metav1.NewTime(now.Add(-time.Hour))
	newer := metav1.NewTime(now)
	pods := []corev1.Pod{
		{Name: "old", CreationTimestamp: older},
		{Name: "new", CreationTimestamp: newer},
		{Name: "failed", CreationTimestamp: metav1.NewTime(now.Add(time.Second)), Status: corev1.PodStatus{Phase: corev1.PodFailed}},
		{Name: "succeeded", CreationTimestamp: metav1.NewTime(now.Add(time.Second)), Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
		{Name: "terminating", CreationTimestamp: newer, DeletionTimestamp: &newer},
	}
	pod, ok := selectNewestActivePod(pods)
	if !ok || pod.Name != "new" {
		t.Fatalf("selectNewestActivePod = %v, %v; want new", pod, ok)
	}
	tie := []corev1.Pod{
		{Name: "aaa", CreationTimestamp: newer},
		{Name: "bbb", CreationTimestamp: newer},
	}
	if tiePod, _ := selectNewestActivePod(tie); tiePod == nil || tiePod.Name != "bbb" {
		t.Fatalf("tie-break = %v, want bbb", tiePod)
	}
	if _, noneOK := selectNewestActivePod(nil); noneOK {
		t.Fatal("expected ok=false for empty input")
	}
}
