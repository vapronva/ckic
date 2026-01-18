package handlers

import (
	"context"
	"maps"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"k8s.io/client-go/kubernetes"

	"gl.vprw.ru/vapronva/ckic/pkg/caddy"
	"gl.vprw.ru/vapronva/ckic/pkg/constants"
	"gl.vprw.ru/vapronva/ckic/pkg/utils"
)

type ConfigHandler struct {
	CommunicationMethod caddy.CommunicationMethod
	Clientset           *kubernetes.Clientset
	Namespace           string
	CaddyImage          string
	EnableLoadBalancer  bool
	DeployedInstances   map[string]*caddy.Instance
	Mu                  *sync.RWMutex
	EnvSecretName       string
	EnvSecretKeys       []string
	DataVolumePVC       string
	ConfigVolumePVC     string
	ExternalEndpoints   utils.ExternalEndpointsMap
	UseHostNetwork      bool
	CaddyAdminOriginKey string
	HTTPHostPort        int
	HTTPSHostPort       int
}

func NewConfigHandler(
	method caddy.CommunicationMethod,
	clientset *kubernetes.Clientset,
	namespace, caddyImage string,
	enableLoadBalancer bool,
	instances map[string]*caddy.Instance,
	mu *sync.RWMutex,
	envSecretName string,
	envSecretKeys []string,
	dataVolumePVC string,
	configVolumePVC string,
	externalEndpoints utils.ExternalEndpointsMap,
	useHostNetwork bool,
	caddyAdminOriginKey string,
	httpHostPort int,
	httpsHostPort int,
) *ConfigHandler {
	return &ConfigHandler{
		CommunicationMethod: method,
		Clientset:           clientset,
		Namespace:           namespace,
		CaddyImage:          caddyImage,
		EnableLoadBalancer:  enableLoadBalancer,
		DeployedInstances:   instances,
		Mu:                  mu,
		EnvSecretName:       envSecretName,
		EnvSecretKeys:       envSecretKeys,
		DataVolumePVC:       dataVolumePVC,
		ConfigVolumePVC:     configVolumePVC,
		ExternalEndpoints:   externalEndpoints,
		UseHostNetwork:      useHostNetwork,
		CaddyAdminOriginKey: caddyAdminOriginKey,
		HTTPHostPort:        httpHostPort,
		HTTPSHostPort:       httpsHostPort,
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
	var wg sync.WaitGroup
	var muFailed sync.Mutex
	var failedNodes []string
	semaphore := make(chan struct{}, 5)
	var apiConfig *caddy.AdminAPIConfig
	if h.CaddyAdminOriginKey != "" {
		apiConfig = &caddy.AdminAPIConfig{
			OriginKey: h.CaddyAdminOriginKey,
		}
	}
	for nodeName, instance := range instancesCopy {
		wg.Add(1)
		go func(nodeName string, instance *caddy.Instance) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			instanceLogger := logger.With().Str("node", nodeName).Logger()
			instanceLogger.Debug().Msg("Updating Caddy configuration")
			var err error
			for retry := range 3 {
				if retry > 0 {
					instanceLogger.Info().Int("retry", retry).Msg("Retrying configuration update")
					time.Sleep(time.Duration(retry*2) * time.Second)
				}
				err = instance.UpdateConfig(context.Background(), configData, h.CommunicationMethod, apiConfig)
				if err == nil {
					break
				}
			}
			if err != nil {
				instanceLogger.Error().Err(err).Msg("Failed to update Caddy configuration")
				newCount := instance.FailureCount.Add(1)
				if newCount >= 5 {
					instanceLogger.Warn().Msg("Too many update failures, marking for redeployment")
					muFailed.Lock()
					failedNodes = append(failedNodes, nodeName)
					muFailed.Unlock()
				}
			} else {
				instanceLogger.Info().Msg("Successfully updated Caddy configuration")
				instance.FailureCount.Store(0)
			}
		}(nodeName, instance)
	}
	wg.Wait()
	if len(failedNodes) > 0 {
		// bearer:disable go_lang_logger_leak
		logger.Info().Msgf("Redeploying %d failed instances", len(failedNodes))
		for _, nodeName := range failedNodes {
			h.Mu.RLock()
			instance := h.DeployedInstances[nodeName]
			h.Mu.RUnlock()
			if instance == nil {
				continue
			}
			logger.Info().Str("node", nodeName).Msg("Redeploying failed Caddy instance")
			if err := instance.Delete(); err != nil {
				logger.Error().Err(err).Str("node", nodeName).Msg("Failed to delete failed Caddy instance")
				continue
			}
			externalIPs := h.ExternalEndpoints[nodeName]
			newInstance, err := caddy.DeployCaddy(
				context.Background(),
				h.Clientset,
				nodeName,
				h.Namespace,
				h.CaddyImage,
				h.EnableLoadBalancer,
				externalIPs,
				h.EnvSecretName,
				h.EnvSecretKeys,
				h.DataVolumePVC,
				h.ConfigVolumePVC,
				h.UseHostNetwork,
				h.HTTPHostPort,
				h.HTTPSHostPort,
			)
			if err != nil {
				logger.Error().Err(err).Str("node", nodeName).Msg("Failed to redeploy Caddy instance")
				continue
			}
			h.Mu.Lock()
			h.DeployedInstances[nodeName] = newInstance
			h.Mu.Unlock()
			logger.Info().Str("node", nodeName).Msg("Successfully redeployed Caddy instance")
		}
	}
}
