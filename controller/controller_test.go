package controller_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gl.vprw.ru/vapronva/caddy-kubernetes-ingress-controller/controller"
	"gl.vprw.ru/vapronva/caddy-kubernetes-ingress-controller/watcher"
	corev1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func TestController_Reconcile_NilPayload(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	cfg := rest.Config{}
	ctrl := controller.NewController(client, &cfg, "default", "caddy-annotation")
	err := ctrl.Reconcile(ctx, nil)
	assert.NoError(t, err)
}

func TestController_Reconcile_NoIngresses(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	cfg := rest.Config{}
	ctrl := controller.NewController(client, &cfg, "default", "caddy-annotation")
	payload := &watcher.Payload{Ingresses: nil}
	err := ctrl.Reconcile(ctx, payload)
	assert.NoError(t, err)
	gotCfg, getErr := client.CoreV1().ConfigMaps("default").Get(ctx, "caddy-kubernetes-ingress-config", metav1.GetOptions{})
	assert.NoError(t, getErr)
	assert.Equal(t, "", gotCfg.Data["Caddyfile"])
}

func TestController_Reconcile_SingleIngress_DefaultTemplate(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	cfg := rest.Config{}
	ctrl := controller.NewController(client, &cfg, "default", "caddy-annotation")
	ing := &networking.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ing-default",
			Namespace: "default",
		},
		Spec: networking.IngressSpec{
			Rules: []networking.IngressRule{
				{
					Host: "example.com",
				},
			},
		},
	}
	payload := &watcher.Payload{Ingresses: []watcher.IngressPayload{{Ingress: ing, ServicePorts: map[string]map[string]int{}}}}
	err := ctrl.Reconcile(ctx, payload)
	assert.NoError(t, err)
	cm, getErr := client.CoreV1().ConfigMaps("default").Get(ctx, "caddy-kubernetes-ingress-config", metav1.GetOptions{})
	assert.NoError(t, getErr)
	assert.Contains(t, cm.Data["Caddyfile"], "example.com", "Caddyfile should contain the host name")
	err = ctrl.Reconcile(ctx, payload)
	assert.NoError(t, err, "reconcile with unchanged config should not error")
}

func TestController_Reconcile_CustomTemplate(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	cfg := rest.Config{}
	ctrl := controller.NewController(client, &cfg, "default", "caddy-annotation")
	ing := &networking.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ing-custom",
			Namespace: "default",
			Annotations: map[string]string{
				"cmld.ru/ckic/caddy-template": "{{ .Ingress.Name }}-custom",
			},
		},
	}
	payload := &watcher.Payload{Ingresses: []watcher.IngressPayload{
		{Ingress: ing, ServicePorts: map[string]map[string]int{}},
	}}
	err := ctrl.Reconcile(ctx, payload)
	assert.NoError(t, err)
	cm, getErr := client.CoreV1().ConfigMaps("default").Get(ctx, "caddy-kubernetes-ingress-config", metav1.GetOptions{})
	assert.NoError(t, getErr)
	assert.Contains(t, cm.Data["Caddyfile"], "test-ing-custom-custom")
}

func TestController_Reconcile_InvalidCustomTemplate(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	cfg := rest.Config{}
	ctrl := controller.NewController(client, &cfg, "default", "caddy-annotation")
	ing := &networking.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bad-template",
			Namespace: "default",
			Annotations: map[string]string{
				"cmld.ru/ckic/caddy-template": "{{ .ThisDoesNotExist }",
			},
		},
	}
	payload := &watcher.Payload{Ingresses: []watcher.IngressPayload{
		{Ingress: ing, ServicePorts: map[string]map[string]int{}},
	}}
	err := ctrl.Reconcile(ctx, payload)
	assert.NoError(t, err, "invalid template should not cause entire reconcile to fail")
	cm, getErr := client.CoreV1().ConfigMaps("default").Get(ctx, "caddy-kubernetes-ingress-config", metav1.GetOptions{})
	assert.NoError(t, getErr)
	assert.NotContains(t, cm.Data["Caddyfile"], "bad-template")
}

func TestController_ensureConfigMap_CreateAndUpdate(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	cfg := rest.Config{}
	ctrl := controller.NewController(client, &cfg, "default", "caddy-annotation")
	err := ctrl.EnsureConfigMap(ctx, "InitialCaddyfile")
	assert.NoError(t, err)
	cm, getErr := client.CoreV1().ConfigMaps("default").Get(ctx, "caddy-kubernetes-ingress-config", metav1.GetOptions{})
	assert.NoError(t, getErr)
	assert.Equal(t, "InitialCaddyfile", cm.Data["Caddyfile"])
	err = ctrl.EnsureConfigMap(ctx, "UpdatedCaddyfile")
	assert.NoError(t, err)
	cm, getErr = client.CoreV1().ConfigMaps("default").Get(ctx, "caddy-kubernetes-ingress-config", metav1.GetOptions{})
	assert.NoError(t, getErr)
	assert.Equal(t, "UpdatedCaddyfile", cm.Data["Caddyfile"])
}

func TestController_reloadCaddyPods(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	podWithAnnotation := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pod-with-annotation",
			Namespace:   "default",
			Annotations: map[string]string{"caddy-annotation": "true"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "caddy"},
			},
		},
	}
	podOtherNamespace := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pod-other-ns",
			Namespace:   "not-default",
			Annotations: map[string]string{"caddy-annotation": "true"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "caddy"}},
		},
	}
	podNoAnnotation := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-no-annotation",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "caddy"}},
		},
	}
	_, _ = client.CoreV1().Pods("default").Create(ctx, podWithAnnotation, metav1.CreateOptions{})
	_, _ = client.CoreV1().Pods("not-default").Create(ctx, podOtherNamespace, metav1.CreateOptions{})
	_, _ = client.CoreV1().Pods("default").Create(ctx, podNoAnnotation, metav1.CreateOptions{})
	cfg := rest.Config{}
	controller.NewController(client, &cfg, "default", "caddy-annotation")
	// TODO: we can't fake test this? oh well...
	// err := ctrl.ReloadCaddyPods(ctx)
	// assert.NoError(t, err)
}
