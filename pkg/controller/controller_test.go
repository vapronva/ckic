package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	cachetesting "k8s.io/client-go/tools/cache/testing"

	"git.horse/vapronva/ckic/pkg/caddy"
)

func newTestController(t *testing.T, client kubernetes.Interface) *Controller {
	t.Helper()
	c, err := NewController(client, Config{BaseConfigMapName: "caddy-config", Namespace: "caddy-system", BootConfigMapName: "merged"})
	if err != nil {
		t.Fatalf("NewController: %v", err)
	}
	t.Cleanup(c.queue.ShutDown)
	return c
}

func addNode(t *testing.T, c *Controller) {
	t.Helper()
	if err := c.nodeFactory.Core().V1().Nodes().Informer().GetStore().Add(&corev1.Node{Name: "node1"}); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileNodePushesOnlyWhenNeeded(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	c, err := NewController(client, Config{
		Namespace: "system", BaseConfigMapName: "base",
		BootConfigMapName: "merged",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.queue.ShutDown)
	addNode(t, c)
	pod := caddy.Pod{IP: "192.0.2.1", ContainerID: "container-1"}
	c.deployFn = func(context.Context, string) (caddy.Pod, error) {
		return pod, nil
	}
	var pushes []string
	c.pushFn = func(_ context.Context, _ caddy.Pod, merged string) error {
		pushes = append(pushes, merged)
		if merged == "rejected\n" {
			return caddy.ErrConfigRejected
		}
		return nil
	}
	reconcile := func(step string, wantPushes int) {
		t.Helper()
		if err := c.reconcileNode(t.Context(), "node1"); err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		if len(pushes) != wantPushes {
			t.Fatalf("%s: pushes = %d, want %d", step, len(pushes), wantPushes)
		}
	}
	reconcile("missing base", 0)
	c.aggregator.UpdateBase("base\n")
	reconcile("first push", 1)
	reconcile("same digest and container", 1)
	c.aggregator.UpdateBase("changed\n")
	reconcile("config change", 2)
	pod.ContainerID = "container-2"
	pod.IP = ""
	reconcile("container restarting", 2)
	pod.IP = "192.0.2.1"
	reconcile("container restarted", 3)
	c.forceConfigResync()
	reconcile("forced resync", 4)
	if pushes[3] != "changed\n" {
		t.Fatalf("pushed %q", pushes[3])
	}
	c.aggregator.UpdateBase("rejected\n")
	reconcile("rejected config", 5)
	reconcile("rejected config on the next trigger", 6)
	if err := c.aggregator.PublishMirror(t.Context()); err != nil {
		t.Fatal(err)
	}
	oversized := strings.Repeat("x", corev1.MaxSecretSize)
	c.aggregator.SetExternal("team/large", oversized)
	reconcile("config that cannot be saved for boot", 6)
	mirror, err := client.CoreV1().ConfigMaps("system").Get(t.Context(), "merged", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if mirror.Data[caddy.CaddyfileKey] != pushes[3] {
		t.Fatal("rejected or oversized config replaced the last loaded boot snapshot")
	}
}

func TestReconcileNodeTearsDownUnmanagedNode(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset(&appsv1.Deployment{Name: "caddy-gone", Namespace: "caddy-system", Labels: caddy.ManagedLabels("gone")})
	c := newTestController(t, client)
	c.deployFn = func(context.Context, string) (caddy.Pod, error) {
		t.Fatal("deployFn must not be called for an unmanaged node")
		return caddy.Pod{}, errors.New("unreachable")
	}
	if err := c.reconcileNode(t.Context(), "gone"); err != nil {
		t.Fatalf("reconcileNode: %v", err)
	}
	if _, err := client.AppsV1().Deployments("caddy-system").Get(t.Context(), "caddy-gone", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected deployment torn down, got %v", err)
	}
}

func TestRunWaitsForBaseAndPushesAllConfigFragments(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset(
		&corev1.Node{Name: "node1"},
		&corev1.ConfigMap{Name: "external", Namespace: "team", Labels: map[string]string{"aggregate": "true"}, Data: map[string]string{caddy.CaddyfileKey: "external"}},
	)
	c, err := NewController(client, Config{
		Namespace: "system", BaseConfigMapName: "base", ExternalEnable: true, ExternalLabel: "aggregate=true",
		BootConfigMapName: "merged",
	})
	if err != nil {
		t.Fatal(err)
	}
	deployed := make(chan struct{}, 1)
	c.deployFn = func(context.Context, string) (caddy.Pod, error) {
		select {
		case deployed <- struct{}{}:
		default:
		}
		return caddy.Pod{IP: "192.0.2.1", ContainerID: "container-1"}, nil
	}
	pushed := make(chan string, 1)
	c.pushFn = func(_ context.Context, _ caddy.Pod, config string) error {
		select {
		case pushed <- config:
		default:
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	select {
	case <-deployed:
	case err := <-done:
		t.Fatalf("Run exited while the base ConfigMap was missing: %v", err)
	case <-ctx.Done():
		t.Fatal("node was not deployed while the base ConfigMap was missing")
	}
	if _, err := client.CoreV1().ConfigMaps("system").Create(ctx, &corev1.ConfigMap{
		Name: "base", Namespace: "system", Data: map[string]string{caddy.CaddyfileKey: "base"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var loaded string
	select {
	case loaded = <-pushed:
	case err := <-done:
		t.Fatalf("Run exited: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("initial config was not pushed")
	}
	if !strings.HasPrefix(loaded, "base\n") || !strings.Contains(loaded, "external") {
		t.Fatalf("initial config omitted informer data: %q", loaded)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		mirror, err := client.CoreV1().ConfigMaps("system").Get(ctx, "merged", metav1.GetOptions{})
		if err == nil && mirror.Data[caddy.CaddyfileKey] == loaded {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("loaded config was not published as the boot snapshot")
		case <-ticker.C:
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("controller did not stop")
	}
}

func TestManagedResourceEventsEnqueueNode(t *testing.T) {
	t.Parallel()
	labels := caddy.ManagedLabels("node1")
	for _, object := range []runtime.Object{
		&corev1.Pod{Name: "caddy", Labels: labels},
		&corev1.Service{Name: "caddy", Labels: labels},
		&appsv1.Deployment{Name: "caddy", Labels: labels},
	} {
		t.Run(fmt.Sprintf("%T", object), func(t *testing.T) {
			t.Parallel()
			c := newTestController(t, fake.NewClientset())
			source := cachetesting.NewFakeControllerSource()
			defer source.Shutdown()
			informer := cache.NewSharedIndexInformer(source, object, 0, cache.Indexers{})
			if err := c.register(informer, c.managedObjectHandler()); err != nil {
				t.Fatal(err)
			}
			go informer.Run(t.Context().Done())
			if !cache.WaitForCacheSync(t.Context().Done(), informer.HasSynced) {
				t.Fatal("cache did not sync")
			}
			for _, event := range []func(runtime.Object){source.Add, source.Modify, source.Delete} {
				event(object.DeepCopyObject())
				enqueued := make(chan string, 1)
				go func() { key, _ := c.queue.Get(); enqueued <- key }()
				select {
				case key := <-enqueued:
					c.queue.Done(key)
					c.queue.Forget(key)
					if key != "node1" {
						t.Fatalf("event enqueued %q", key)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("managed resource event was ignored")
				}
			}
		})
	}
}

func TestBootSnapshotKeepsLoadedConfigAcrossWriteFailures(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	c := newTestController(t, client)
	addNode(t, c)
	if err := c.aggregator.InitializeMirror(t.Context(), "bootstrap"); err != nil {
		t.Fatal(err)
	}
	c.deployFn = func(context.Context, string) (caddy.Pod, error) {
		return caddy.Pod{IP: "192.0.2.1", ContainerID: "container-1"}, nil
	}
	pushes := 0
	c.pushFn = func(_ context.Context, _ caddy.Pod, merged string) error {
		pushes++
		if merged != "loaded\n" {
			return caddy.ErrConfigRejected
		}
		c.aggregator.UpdateBase("broken\n")
		return nil
	}
	writeError := apierrors.NewServiceUnavailable("etcd unavailable")
	isWriteFailing := true
	client.PrependReactor("patch", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		if isWriteFailing {
			return true, nil, writeError
		}
		return false, nil, nil
	})
	c.aggregator.UpdateBase("loaded\n")
	if err := c.reconcileNode(t.Context(), "node1"); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcile(t.Context(), mirrorKey); !errors.Is(err, writeError) {
		t.Fatalf("boot snapshot write = %v", err)
	}
	if err := c.reconcileNode(t.Context(), "node1"); err != nil {
		t.Fatalf("reconcile of rejected config = %v", err)
	}
	isWriteFailing = false
	if err := c.reconcile(t.Context(), mirrorKey); err != nil {
		t.Fatal(err)
	}
	mirror, err := client.CoreV1().ConfigMaps(c.config.Namespace).Get(t.Context(), "merged", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := mirror.Data[caddy.CaddyfileKey]; got != "loaded\n" {
		t.Fatalf("boot snapshot = %q, want the config Caddy loaded", got)
	}
	if pushes != 2 {
		t.Fatalf("pushes = %d, want 2: boot snapshot retries must not reload Caddy", pushes)
	}
}

func TestRolloutPrePullsEverywhereThenRollsOneNodeAtATime(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	c, err := NewController(client, Config{
		Namespace: "system", BaseConfigMapName: "base", BootConfigMapName: "merged",
		Deploy: caddy.DeployOptions{CaddyImage: "caddy:new", PrePullImage: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.queue.ShutDown)
	nodes := c.nodeFactory.Core().V1().Nodes().Informer().GetStore()
	deployments := c.nsFactory.Apps().V1().Deployments().Informer().GetStore()
	pods := c.nsFactory.Core().V1().Pods().Informer().GetStore()
	setDeployment := func(node, hash string, isAvailable bool) {
		t.Helper()
		deployment := &appsv1.Deployment{
			Name: caddy.DeploymentName(node), Namespace: "system", Generation: 2,
			Annotations: map[string]string{"ckic.cmld.ru/template-hash": hash},
		}
		deployment.Spec.Template.Spec.Containers = []corev1.Container{{Name: "caddy", Image: "caddy:old"}}
		deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, UpdatedReplicas: 1}
		if isAvailable {
			deployment.Status.AvailableReplicas = 1
		}
		if err := deployments.Update(deployment); err != nil {
			t.Fatal(err)
		}
	}
	syncPrePulls := func() {
		t.Helper()
		list, err := client.CoreV1().Pods("system").List(t.Context(), metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, pod := range pods.List() {
			if err := pods.Delete(pod); err != nil {
				t.Fatal(err)
			}
		}
		for i := range list.Items {
			list.Items[i].Status.Phase = corev1.PodSucceeded
			list.Items[i].Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: "prepull", ContainerID: "containerd://prepull",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}},
			}}
			if err := pods.Add(&list.Items[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
	isHeld := func(node string) bool {
		t.Helper()
		held, err := c.syncRollout(t.Context(), node)
		if err != nil {
			t.Fatal(err)
		}
		return held
	}
	for _, node := range []string{"a", "b"} {
		if err := nodes.Add(&corev1.Node{Name: node}); err != nil {
			t.Fatal(err)
		}
		setDeployment(node, "outdated", true)
	}
	if !isHeld("a") || !isHeld("b") {
		t.Fatal("a node rolled out before every node pre-pulled the image")
	}
	syncPrePulls()
	if len(pods.List()) != 2 {
		t.Fatalf("pre-pull pods = %d, want one per pending node", len(pods.List()))
	}
	if !isHeld("b") || isHeld("a") {
		t.Fatal("pending nodes must roll out one at a time in name order")
	}
	setDeployment("a", caddy.TemplateHash(c.config.Deploy, "a"), false)
	if !isHeld("b") {
		t.Fatal("b rolled out while a was still rolling out")
	}
	if isHeld("a") {
		t.Fatal("a rolled out node must not be held")
	}
	syncPrePulls()
	if len(pods.List()) != 1 {
		t.Fatal("rolled out node kept its pre-pull pod")
	}
	setDeployment("a", caddy.TemplateHash(c.config.Deploy, "a"), true)
	if isHeld("b") {
		t.Fatal("b was held after a finished rolling out")
	}
	setDeployment("b", caddy.TemplateHash(c.config.Deploy, "b"), false)
	c.config.Deploy.CaddyImage = "caddy:newest"
	for range 2 {
		isHeld("a")
		isHeld("b")
		syncPrePulls()
	}
	if !isHeld("a") || isHeld("b") {
		t.Fatal("a target change mid-rollout must finish the unavailable node before touching available ones")
	}
}
