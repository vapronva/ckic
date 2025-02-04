package watcher_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gl.vprw.ru/vapronva/caddy-kubernetes-ingress-controller/watcher"
	networking "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestWatcherRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := fake.NewSimpleClientset()
	var mu sync.Mutex
	var receivedPayload *watcher.Payload
	w := watcher.New(client, func(p *watcher.Payload) {
		mu.Lock()
		defer mu.Unlock()
		receivedPayload = p
	})
	go func() {
		err := w.Run(ctx)
		assert.NoError(t, err, "unexpected error while running watcher")
	}()
	ing := &networking.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ingress",
			Namespace: "default",
		},
		Spec: networking.IngressSpec{},
	}
	_, err := client.NetworkingV1().Ingresses("default").Create(ctx, ing, metav1.CreateOptions{})
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	mu.Lock()
	assert.NotNil(t, receivedPayload, "payload should have been set after ingress creation")
	if receivedPayload != nil {
		assert.Equal(t, 1, len(receivedPayload.Ingresses), "should have exactly one ingress in the payload")
		assert.Equal(t, "test-ingress", receivedPayload.Ingresses[0].Ingress.Name)
	}
	mu.Unlock()
	ing.Labels = map[string]string{"updated": "true"}
	_, err = client.NetworkingV1().Ingresses("default").Update(ctx, ing, metav1.UpdateOptions{})
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	mu.Lock()
	assert.NotNil(t, receivedPayload, "payload should have been updated after ingress update")
	if receivedPayload != nil {
		assert.Equal(t, 1, len(receivedPayload.Ingresses), "should still have exactly one ingress in the payload")
		assert.Equal(t, "test-ingress", receivedPayload.Ingresses[0].Ingress.Name)
		assert.Equal(t, "true", receivedPayload.Ingresses[0].Ingress.Labels["updated"])
	}
	mu.Unlock()
	err = client.NetworkingV1().Ingresses("default").Delete(ctx, "test-ingress", metav1.DeleteOptions{})
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	mu.Lock()
	assert.NotNil(t, receivedPayload, "payload should not be nil after ingress deletion")
	if receivedPayload != nil {
		assert.Equal(t, 0, len(receivedPayload.Ingresses), "no ingresses should remain after delete")
	}
	mu.Unlock()
}
