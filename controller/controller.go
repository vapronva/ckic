package controller

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"text/template"

	"github.com/rs/zerolog/log"
	"gl.vprw.ru/vapronva/caddy-kubernetes-ingress-controller/templates"
	"gl.vprw.ru/vapronva/caddy-kubernetes-ingress-controller/watcher"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const customTemplateAnnotation = "cmld.ru/ckic/caddy-template"

type Controller struct {
	client              kubernetes.Interface
	cfg                 rest.Config
	namespace           string
	containerAnnotation string
	lastConfigHash      uint64
}

func NewController(
	client kubernetes.Interface,
	cfg *rest.Config,
	namespace string,
	containerAnnotation string,
) *Controller {
	return &Controller{
		client:              client,
		cfg:                 *cfg,
		namespace:           namespace,
		containerAnnotation: containerAnnotation,
		lastConfigHash:      0,
	}
}

func (c *Controller) Reconcile(ctx context.Context, payload *watcher.Payload) error {
	if payload == nil {
		log.Info().Msg("payload is nil; nothing to reconcile")
		return nil
	}
	log.Info().Msg("reconciling caddy configuration")
	log.Debug().Msgf("payload: %+v", payload)
	tpl := templates.DefaultCaddyfileTemplate
	for _, ing := range payload.Ingresses {
		if ingTmpl, ok := ing.Ingress.Annotations[customTemplateAnnotation]; ok && ingTmpl != "" {
			tpl = ingTmpl
			break
		}
	}
	rendered, err := c.renderCaddyfile(tpl, payload)
	if err != nil {
		return fmt.Errorf("failed to render caddyfile template: %w", err)
	}
	cfgHash := hashString(rendered)
	if cfgHash == c.lastConfigHash {
		log.Debug().Msg("caddyfile has not changed; skipping update/reload")
		return nil
	}
	log.Info().Msg("caddyfile changed; updating configmap and reloading caddy pods")
	if err := c.ensureConfigMap(ctx, rendered); err != nil {
		return fmt.Errorf("failed to ensure caddy configmap: %w", err)
	}
	if err := c.reloadCaddyPods(ctx); err != nil {
		return fmt.Errorf("failed to reload caddy pods: %w", err)
	}
	c.lastConfigHash = cfgHash
	return nil
}

func (c *Controller) renderCaddyfile(tplStr string, payload *watcher.Payload) (string, error) {
	tpl, err := template.New("caddyfile").Parse(tplStr)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, payload); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (c *Controller) ensureConfigMap(ctx context.Context, caddyfile string) error {
	cmName := "caddy-kubernetes-ingress-config"
	dataKey := "Caddyfile"
	existing, err := c.client.CoreV1().ConfigMaps(c.namespace).Get(ctx, cmName, metav1.GetOptions{})
	log.Debug().Msgf("existing configmap: %+v", existing)
	if err != nil {
		log.Debug().Err(err).Msg("failed to get existing configmap")
	}
	if err == nil {
		existing.Data[dataKey] = caddyfile
		_, updateErr := c.client.CoreV1().ConfigMaps(c.namespace).Update(ctx, existing, metav1.UpdateOptions{})
		log.Debug().Err(updateErr).Msg("updating existing configmap")
		return updateErr
	}
	newCm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: c.namespace,
		},
		Data: map[string]string{
			dataKey: caddyfile,
		},
	}
	log.Debug().Msgf("creating new configmap: %+v", newCm)
	_, createErr := c.client.CoreV1().ConfigMaps(c.namespace).Create(ctx, newCm, metav1.CreateOptions{})
	return createErr
}

func (c *Controller) reloadCaddyPods(ctx context.Context) error {
	podList, err := c.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	reloadCommand := []string{
		"caddy",
		"reload",
		"--config",
		"/etc/caddy/Caddyfile",
	}
	for _, pod := range podList.Items {
		if pod.Namespace != c.namespace {
			continue
		}
		if _, ok := pod.Annotations[c.containerAnnotation]; !ok {
			continue
		}
		var containerName string
		if len(pod.Spec.Containers) == 1 {
			containerName = pod.Spec.Containers[0].Name
		} else {
			for _, ctr := range pod.Spec.Containers {
				if ctr.Name == "caddy" {
					containerName = ctr.Name
					break
				}
			}
			if containerName == "" && len(pod.Spec.Containers) > 0 {
				containerName = pod.Spec.Containers[0].Name
			}
		}
		log.Info().
			Str("pod", pod.Name).
			Str("namespace", pod.Namespace).
			Msg("reloading caddy in pod")
		if err := ExecCmdInPod(ctx, c.client, c.cfg, pod.Namespace, pod.Name, containerName, reloadCommand); err != nil {
			log.Error().Err(err).
				Str("pod", pod.Name).
				Str("namespace", pod.Namespace).
				Msg("failed caddy reload command")
		}
	}

	return nil
}

func hashString(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
