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
	c.deployFn = func(context.Context, *corev1.Node) (caddy.Pod, error) {
		return pod, nil
	}
	var pushes []string
	c.pushFn = func(_ context.Context, _, merged string) error {
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
	pod.ContainerID = ""
	reconcile("container restarting", 2)
	pod.ContainerID = "container-2"
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
	c.deployFn = func(context.Context, *corev1.Node) (caddy.Pod, error) {
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
	c.deployFn = func(context.Context, *corev1.Node) (caddy.Pod, error) {
		select {
		case deployed <- struct{}{}:
		default:
		}
		return caddy.Pod{IP: "192.0.2.1", ContainerID: "container-1"}, nil
	}
	pushed := make(chan string, 1)
	c.pushFn = func(_ context.Context, _, config string) error {
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
	c.deployFn = func(context.Context, *corev1.Node) (caddy.Pod, error) {
		return caddy.Pod{IP: "192.0.2.1", ContainerID: "container-1"}, nil
	}
	pushes := 0
	c.pushFn = func(_ context.Context, _, merged string) error {
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
	setAPIDeployment := func(node, image string, isAvailable bool) *appsv1.Deployment {
		t.Helper()
		opts := c.config.Deploy
		opts.CaddyImage = image
		deployment := &appsv1.Deployment{
			Name: caddy.DeploymentName(node), Namespace: "system", Generation: 2, Labels: caddy.ManagedLabels(node),
			Annotations: map[string]string{"ckic.cmld.ru/template-hash": caddy.TemplateHash(opts, node)},
		}
		deployment.Spec.Template.Spec.Containers = []corev1.Container{{Name: "caddy", Image: image}}
		deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1}
		if !isAvailable {
			deployment.Generation = 3
		}
		api := client.AppsV1().Deployments("system")
		_, err := api.Update(t.Context(), deployment.DeepCopy(), metav1.UpdateOptions{})
		if apierrors.IsNotFound(err) {
			_, err = api.Create(t.Context(), deployment.DeepCopy(), metav1.CreateOptions{})
		}
		if err != nil {
			t.Fatal(err)
		}
		return deployment
	}
	setDeployment := func(node, image string, isAvailable bool) {
		t.Helper()
		if err := deployments.Update(setAPIDeployment(node, image, isAvailable)); err != nil {
			t.Fatal(err)
		}
	}
	setNode := func(name string, isReady bool) {
		t.Helper()
		ready := corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionFalse}
		if isReady {
			ready.Status = corev1.ConditionTrue
		}
		if err := nodes.Update(&corev1.Node{Name: name, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{ready}}}); err != nil {
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
	isHeld := func(name string) bool {
		t.Helper()
		node, err := c.nodeLister.Get(name)
		if err != nil {
			t.Fatal(err)
		}
		held, err := c.syncRollout(t.Context(), node)
		if err != nil {
			t.Fatal(err)
		}
		return held
	}
	for _, node := range []string{"a", "b", "c"} {
		setNode(node, node != "a")
		setDeployment(node, "caddy:old", node != "a")
	}
	if !isHeld("a") || !isHeld("b") || !isHeld("c") {
		t.Fatal("a node rolled out before every node pre-pulled the image")
	}
	syncPrePulls()
	if len(pods.List()) != 2 {
		t.Fatalf("pre-pull pods = %d, want one per pending node in service", len(pods.List()))
	}
	if !isHeld("a") {
		t.Fatal("a node that is not ready was rolled out")
	}
	if !isHeld("c") || isHeld("b") {
		t.Fatal("pending nodes in service must roll out one at a time in name order")
	}
	setAPIDeployment("b", "caddy:new", false)
	setNode("a", true)
	setDeployment("a", "caddy:old", true)
	isHeld("a")
	syncPrePulls()
	if !isHeld("a") {
		t.Fatal("a node returning to service rolled out while the cache still hid the rollout of b")
	}
	setDeployment("b", "caddy:new", false)
	if !isHeld("c") {
		t.Fatal("c rolled out while b was still rolling out")
	}
	if isHeld("b") {
		t.Fatal("a rolled out node must not be held")
	}
	syncPrePulls()
	if len(pods.List()) != 2 {
		t.Fatal("rolled out node kept its pre-pull pod")
	}
	setDeployment("b", "caddy:new", true)
	if !isHeld("c") || isHeld("a") {
		t.Fatal("a returned node must take its turn in name order")
	}
	setDeployment("a", "caddy:new", false)
	c.config.Deploy.CaddyImage = "caddy:newest"
	for range 2 {
		isHeld("a")
		isHeld("b")
		isHeld("c")
		syncPrePulls()
	}
	if !isHeld("b") || isHeld("a") {
		t.Fatal("a target change mid-rollout must finish the unavailable node before touching available ones")
	}
}
