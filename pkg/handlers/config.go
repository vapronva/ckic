package handlers

import (
	"maps"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"k8s.io/client-go/kubernetes"

	"gl.vprw.ru/vapronva/ckic/pkg/caddy"
	"gl.vprw.ru/vapronva/ckic/pkg/constants"
)

type ConfigHandler struct {
	CommunicationMethod caddy.CommunicationMethod
	Clientset           *kubernetes.Clientset
	Namespace           string
	CaddyImage          string
	EnableNodePort      bool
	DeployedInstances   map[string]*caddy.Instance
	Mu                  *sync.RWMutex
}

func NewConfigHandler(method caddy.CommunicationMethod, clientset *kubernetes.Clientset, namespace, caddyImage string, enableNodePort bool, instances map[string]*caddy.Instance, mu *sync.RWMutex) *ConfigHandler {
	return &ConfigHandler{
		CommunicationMethod: method,
		Clientset:           clientset,
		Namespace:           namespace,
		CaddyImage:          caddyImage,
		EnableNodePort:      enableNodePort,
		DeployedInstances:   instances,
		Mu:                  mu,
	}
}

func (h *ConfigHandler) Handle(configData string) {
	logger := log.With().Str("component", "config_handler").Logger()
	logger.Info().Msg("Detected configuration change, updating Caddy instances")
	time.Sleep(constants.ConfigUpdateDelay)
	h.Mu.RLock()
	instancesCopy := make(map[string]*caddy.Instance)
	maps.Copy(instancesCopy, h.DeployedInstances)
	h.Mu.RUnlock()
	for nodeName, instance := range instancesCopy {
		instanceLogger := logger.With().Str("node", nodeName).Logger()
		instanceLogger.Debug().Msg("Updating Caddy configuration")
		err := instance.UpdateConfig(configData, h.CommunicationMethod)
		if err != nil {
			instanceLogger.Error().Err(err).Msg("Failed to update Caddy configuration")
			if instance.FailureCount >= 5 {
				instanceLogger.Warn().Msg("Too many update failures, redeploying instance")
				if err := instance.Delete(); err != nil {
					instanceLogger.Error().Err(err).Msg("Failed to delete failed Caddy instance")
					continue
				}
				newInstance, err := caddy.DeployCaddy(h.Clientset, nodeName, h.Namespace, h.CaddyImage, h.EnableNodePort)
				if err != nil {
					instanceLogger.Error().Err(err).Msg("Failed to redeploy Caddy instance")
					continue
				}
				h.Mu.Lock()
				h.DeployedInstances[nodeName] = newInstance
				h.Mu.Unlock()
				instanceLogger.Info().Msg("Successfully redeployed Caddy instance")
			}
		} else {
			instanceLogger.Info().Msg("Successfully updated Caddy configuration")
			instance.FailureCount = 0
		}
	}
}
