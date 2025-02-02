package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gl.vprw.ru/vapronva/caddy-kubernetes-ingress-controller/watcher"
	networking "k8s.io/api/networking/v1"
)

func TestRoutingTable(t *testing.T) {
	t.Run("empty payload", func(t *testing.T) {
		rt := NewRoutingTable(nil)
		u, err := rt.GetBackend("host", "/")
		assert.Nil(t, u)
		assert.Error(t, err)
	})
	t.Run("default backend with no rules", func(t *testing.T) {
		rt := NewRoutingTable(&watcher.Payload{
			Ingresses: []watcher.IngressPayload{{
				Ingress: &networking.Ingress{Spec: networking.IngressSpec{
					DefaultBackend: &networking.IngressBackend{
						Service: &networking.IngressServiceBackend{
							Name: "caddy-1.example.svc.cluster.local",
							Port: networking.ServiceBackendPort{
								Number: 80,
							},
						},
					},
				}},
			}},
		})
		u, err := rt.GetBackend("likes-sniffing-f.art", "/")
		assert.Error(t, err)
		assert.Nil(t, u)
	})
}
