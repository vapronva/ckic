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
	c, err := NewController(client, Config{ConfigMapName: "caddy-config", Namespace: "caddy-system", ExternalAggregatedConfigName: "merged"})
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
		Namespace: "system", ConfigMapName: "base",
		ExternalAggregatedConfigName: "merged",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.queue.ShutDown)
	addNode(t, c)
	pod := caddy.Pod{IP: "192.0.2.1", ContainerID: "container-1"}
	c.deployFn = func(context.Context, caddy.DeployOptions, string, []string) (caddy.Pod, error) {
		return pod, nil
	}
	var pushes []string
	c.pushFn = func(_ context.Context, _ caddy.Pod, merged string) error {
		pushes = append(pushes, merged)
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
	if err := c.reconcileNode(t.Context(), "node1"); err == nil {
		t.Fatal("reconcile without a base Caddyfile must fail")
	}
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
	if err := c.aggregator.PublishMirror(t.Context()); err != nil {
		t.Fatal(err)
	}
	oversized := strings.Repeat("x", corev1.MaxSecretSize)
	c.aggregator.SetExternal("team/large", oversized)
	if err := c.reconcileNode(t.Context(), "node1"); err == nil {
		t.Fatal("oversized config was accepted")
	}
	if len(pushes) != 4 {
		t.Fatal("config that cannot be saved for boot was pushed")
	}
	mirror, err := client.CoreV1().ConfigMaps("system").Get(t.Context(), "merged", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if mirror.Data[caddy.CaddyfileKey] != pushes[3] {
		t.Fatal("oversized config replaced the last fitting boot snapshot")
	}
}

func TestReconcileNodeTearsDownUnmanagedNode(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset(&appsv1.Deployment{Name: "caddy-gone", Namespace: "caddy-system", Labels: caddy.ManagedLabels("gone")})
	c := newTestController(t, client)
	c.deployFn = func(context.Context, caddy.DeployOptions, string, []string) (caddy.Pod, error) {
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
		Namespace: "system", ConfigMapName: "base", ExternalEnable: true, ExternalLabel: "aggregate=true",
		ExternalAggregatedConfigName: "merged",
	})
	if err != nil {
		t.Fatal(err)
	}
	c.deployFn = func(context.Context, caddy.DeployOptions, string, []string) (caddy.Pod, error) {
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
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for c.queue.NumRequeues("node1") == 0 {
		select {
		case err := <-done:
			t.Fatalf("Run exited while the base ConfigMap was missing: %v", err)
		case <-ctx.Done():
			t.Fatal("missing base ConfigMap was not retried")
		case <-ticker.C:
		}
	}
	if _, err := client.CoreV1().ConfigMaps("system").Create(ctx, &corev1.ConfigMap{
		Name: "base", Namespace: "system", Data: map[string]string{caddy.CaddyfileKey: "base"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case config := <-pushed:
		if !strings.HasPrefix(config, "base\n") || !strings.Contains(config, "external") {
			t.Fatalf("initial config omitted informer data: %q", config)
		}
	case err := <-done:
		t.Fatalf("Run exited: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("initial config was not pushed")
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

func TestMirrorRetryDoesNotReloadCaddy(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	c := newTestController(t, client)
	addNode(t, c)
	if err := c.aggregator.InitializeMirror(t.Context(), "bootstrap"); err != nil {
		t.Fatal(err)
	}
	c.aggregator.UpdateBase("accepted\n")
	c.deployFn = func(context.Context, caddy.DeployOptions, string, []string) (caddy.Pod, error) {
		return caddy.Pod{IP: "192.0.2.1", ContainerID: "container-1"}, nil
	}
	pushes := 0
	c.pushFn = func(context.Context, caddy.Pod, string) error { pushes++; return nil }
	writeError := apierrors.NewForbidden(corev1.Resource("configmaps"), "merged", errors.New("patch denied"))
	shouldFail := true
	client.PrependReactor("patch", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		if shouldFail {
			return true, nil, writeError
		}
		return false, nil, nil
	})
	for range 2 {
		if err := c.reconcileNode(t.Context(), "node1"); !errors.Is(err, writeError) {
			t.Fatalf("mirror write failure = %v", err)
		}
	}
	if pushes != 1 {
		t.Fatalf("mirror failure caused %d Caddy reloads, want 1", pushes)
	}
	shouldFail = false
	if err := c.reconcileNode(t.Context(), "node1"); err != nil {
		t.Fatal(err)
	}
	mirror, err := client.CoreV1().ConfigMaps(c.config.Namespace).Get(t.Context(), "merged", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if mirror.Data[caddy.CaddyfileKey] != "accepted\n" {
		t.Fatal("mirror retry did not persist the accepted config")
	}
	if pushes != 1 {
		t.Fatal("mirror recovery reloaded an unchanged Caddy config")
	}
}

func TestLoadedConfigIsMirroredDespiteDrift(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	c := newTestController(t, client)
	addNode(t, c)
	if err := c.aggregator.InitializeMirror(t.Context(), "bootstrap"); err != nil {
		t.Fatal(err)
	}
	c.deployFn = func(context.Context, caddy.DeployOptions, string, []string) (caddy.Pod, error) {
		return caddy.Pod{IP: "192.0.2.1", ContainerID: "container-1"}, nil
	}
	rejected := errors.New("Caddy rejected config")
	c.pushFn = func(_ context.Context, _ caddy.Pod, merged string) error {
		if merged != "loaded\n" {
			return rejected
		}
		c.aggregator.UpdateBase("broken\n")
		return nil
	}
	mirror := func() string {
		t.Helper()
		cm, err := client.CoreV1().ConfigMaps(c.config.Namespace).Get(t.Context(), "merged", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return cm.Data[caddy.CaddyfileKey]
	}
	c.aggregator.UpdateBase("loaded\n")
	if err := c.reconcileNode(t.Context(), "node1"); err != nil {
		t.Fatal(err)
	}
	if got := mirror(); got != "loaded\n" {
		t.Fatalf("boot snapshot = %q, want the config Caddy loaded", got)
	}
	if err := c.reconcileNode(t.Context(), "node1"); !errors.Is(err, rejected) {
		t.Fatalf("reconcile of rejected config = %v", err)
	}
	if got := mirror(); got != "loaded\n" {
		t.Fatalf("boot snapshot = %q after a rejected push", got)
	}
}
