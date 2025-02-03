package watcher

import (
	"context"
	"sync"
	"time"

	"github.com/bep/debounce"
	"github.com/rs/zerolog/log"
	networking "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type Payload struct {
	Ingresses []IngressPayload
}

type IngressPayload struct {
	Ingress      *networking.Ingress
	ServicePorts map[string]map[string]int
}

type Watcher struct {
	client   kubernetes.Interface
	onChange func(*Payload)
}

func New(client kubernetes.Interface, onChange func(*Payload)) *Watcher {
	return &Watcher{
		client:   client,
		onChange: onChange,
	}
}

func (w *Watcher) Run(ctx context.Context) error {
	factory := informers.NewSharedInformerFactory(w.client, time.Minute)
	serviceLister := factory.Core().V1().Services().Lister()
	ingressLister := factory.Networking().V1().Ingresses().Lister()
	addBackend := func(ingressPayload *IngressPayload, backend networking.IngressBackend) {
		svc, err := serviceLister.Services(ingressPayload.Ingress.Namespace).Get(backend.Service.Name)
		if err != nil {
			log.Error().Err(err).
				Str("namespace", ingressPayload.Ingress.Namespace).
				Str("name", backend.Service.Name).
				Msg("unknown service")
		} else {
			m := make(map[string]int)
			for _, port := range svc.Spec.Ports {
				m[port.Name] = int(port.Port)
			}
			ingressPayload.ServicePorts[svc.Name] = m
		}
	}
	onChange := func() {
		payload := &Payload{}
		ingresses, err := ingressLister.List(labels.Everything())
		if err != nil {
			log.Error().Err(err).Msg("failed to list ingresses")
			return
		}
		for _, ingress := range ingresses {
			ingressPayload := IngressPayload{
				Ingress:      ingress,
				ServicePorts: make(map[string]map[string]int),
			}
			payload.Ingresses = append(payload.Ingresses, ingressPayload)
			if ingress.Spec.DefaultBackend != nil {
				addBackend(&ingressPayload, *ingress.Spec.DefaultBackend)
			}
			for _, rule := range ingress.Spec.Rules {
				if rule.HTTP == nil {
					continue
				}
				for _, path := range rule.HTTP.Paths {
					addBackend(&ingressPayload, path.Backend)
				}
			}
		}
		w.onChange(payload)
	}
	debounced := debounce.New(time.Second)
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			debounced(onChange)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			debounced(onChange)
		},
		DeleteFunc: func(obj interface{}) {
			debounced(onChange)
		},
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		informer := factory.Networking().V1().Ingresses().Informer()
		informer.AddEventHandler(handler)
		informer.Run(ctx.Done())
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		informer := factory.Core().V1().Services().Informer()
		informer.AddEventHandler(handler)
		informer.Run(ctx.Done())
		wg.Done()
	}()
	wg.Wait()
	return nil
}
