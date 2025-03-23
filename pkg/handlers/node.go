package handlers

import (
	"sync"

	"github.com/rs/zerolog/log"
	"k8s.io/client-go/kubernetes"

	"gl.vprw.ru/vapronva/ckic/pkg/caddy"
	"gl.vprw.ru/vapronva/ckic/pkg/watcher"
)

type NodeHandler struct {
	Clientset          *kubernetes.Clientset
	Namespace          string
	CaddyImage         string
	EnableLoadBalancer bool
	DeployedInstances  map[string]*caddy.Instance
	Mu                 *sync.RWMutex
}

func NewNodeHandler(clientset *kubernetes.Clientset, namespace, caddyImage string, enableLoadBalancer bool, instances map[string]*caddy.Instance, mu *sync.RWMutex) *NodeHandler {
	return &NodeHandler{
		Clientset:          clientset,
		Namespace:          namespace,
		CaddyImage:         caddyImage,
		EnableLoadBalancer: enableLoadBalancer,
		DeployedInstances:  instances,
		Mu:                 mu,
	}
}

func (h *NodeHandler) Handle(event watcher.NodeEvent) {
	nodeName := event.NodeName
	logger := log.With().Str("node", nodeName).Logger()
	h.Mu.Lock()
	defer h.Mu.Unlock()
	switch event.Type {
	case watcher.NodeAdded:
		logger.Info().Msg("Detected new node, deploying Caddy")
		instance, err := caddy.DeployCaddy(h.Clientset, nodeName, h.Namespace, h.CaddyImage, h.EnableLoadBalancer)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to deploy Caddy instance")
			return
		}
		h.DeployedInstances[nodeName] = instance
		logger.Info().Msg("Successfully deployed Caddy instance")
	case watcher.NodeRemoved:
		if instance, exists := h.DeployedInstances[nodeName]; exists {
			logger.Info().Msg("Node removed, cleaning up Caddy instance")
			if err := instance.Delete(); err != nil {
				logger.Error().Err(err).Msg("Failed to delete Caddy instance")
			} else {
				delete(h.DeployedInstances, nodeName)
				logger.Info().Msg("Successfully removed Caddy instance")
			}
		}
	}
}
