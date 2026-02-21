package handlers

import (
	"context"
	"maps"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/caddy"
	"git.horse/vapronva/ckic/pkg/constants"
	"git.horse/vapronva/ckic/pkg/utils"
)

type ConfigHandler struct {
	CommunicationMethod caddy.CommunicationMethod
	Clientset           *kubernetes.Clientset
	Namespace           string
	CaddyImage          string
	EnableLoadBalancer  bool
	DeployedInstances   map[string]*caddy.Instance
	Mu                  *sync.RWMutex
	handleMu            sync.Mutex
	EnvSecretName       string
	EnvSecretKeys       []string
	DataVolumePVC       string
	ConfigVolumePVC     string
	ConfigMapName       string
	ExternalEndpoints   utils.ExternalEndpointsMap
	UseHostNetwork      bool
	CaddyAdminOriginKey string
	HTTPHostPort        int
	HTTPSHostPort       int
}

type failedInstance struct {
	nodeName string
	instance *caddy.Instance
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
	configMapName string,
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
		ConfigMapName:       configMapName,
		ExternalEndpoints:   externalEndpoints,
		UseHostNetwork:      useHostNetwork,
		CaddyAdminOriginKey: caddyAdminOriginKey,
		HTTPHostPort:        httpHostPort,
		HTTPSHostPort:       httpsHostPort,
	}
}

func (h *ConfigHandler) Handle(configData string) {
	h.handleMu.Lock()
	defer h.handleMu.Unlock()
	logger := log.With().Str("component", "config_handler").Logger()
	logger.Info().Msg("Detected configuration change, updating Caddy instances")
	time.Sleep(constants.ConfigUpdateDelay)
	h.Mu.RLock()
	instancesCopy := make(map[string]*caddy.Instance)
	maps.Copy(instancesCopy, h.DeployedInstances)
	h.Mu.RUnlock()
	var wg sync.WaitGroup
	var muFailed sync.Mutex
	var failedInstances []failedInstance
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
				var newCount int32
				h.Mu.RLock()
				current := h.DeployedInstances[nodeName] == instance
				if current {
					newCount = instance.FailureCount.Add(1)
				}
				h.Mu.RUnlock()
				if !current {
					instanceLogger.Debug().Msg("Instance replaced during update, skipping failure tracking")
					return
				}
				if newCount >= 5 {
					instanceLogger.Warn().Msg("Too many update failures, marking for redeployment")
					muFailed.Lock()
					failedInstances = append(failedInstances, failedInstance{
						nodeName: nodeName,
						instance: instance,
					})
					muFailed.Unlock()
				}
			} else {
				instanceLogger.Info().Msg("Successfully updated Caddy configuration")
				h.Mu.RLock()
				current := h.DeployedInstances[nodeName] == instance
				if current {
					instance.FailureCount.Store(0)
				}
				h.Mu.RUnlock()
				if !current {
					instanceLogger.Debug().Msg("Instance replaced during update, skipping failure reset")
				}
			}
		}(nodeName, instance)
	}
	wg.Wait()
	if len(failedInstances) > 0 {
		logger.Info().Msgf("Redeploying %d failed instances", len(failedInstances))
		for _, failed := range failedInstances {
			h.Mu.Lock()
			current := h.DeployedInstances[failed.nodeName]
			if current == nil {
				h.Mu.Unlock()
				logger.Debug().Str("node", failed.nodeName).Msg("Failed instance no longer exists, skipping redeployment")
				continue
			}
			if current != failed.instance {
				h.Mu.Unlock()
				logger.Debug().Str("node", failed.nodeName).Msg("Failed instance was replaced, skipping redeployment")
				continue
			}
			logger.Info().Str("node", failed.nodeName).Msg("Redeploying failed Caddy instance")
			if err := failed.instance.Delete(); err != nil {
				h.Mu.Unlock()
				logger.Error().Err(err).Str("node", failed.nodeName).Msg("Failed to delete failed Caddy instance")
				continue
			}
			externalIPs := h.ExternalEndpoints[failed.nodeName]
			newInstance, err := caddy.DeployCaddy(
				context.Background(),
				h.Clientset,
				failed.nodeName,
				h.Namespace,
				h.CaddyImage,
				h.EnableLoadBalancer,
				externalIPs,
				h.EnvSecretName,
				h.EnvSecretKeys,
				h.DataVolumePVC,
				h.ConfigVolumePVC,
				h.ConfigMapName,
				h.UseHostNetwork,
				h.HTTPHostPort,
				h.HTTPSHostPort,
			)
			if err != nil {
				h.Mu.Unlock()
				logger.Error().Err(err).Str("node", failed.nodeName).Msg("Failed to redeploy Caddy instance")
				continue
			}
			h.DeployedInstances[failed.nodeName] = newInstance
			h.Mu.Unlock()
			logger.Info().Str("node", failed.nodeName).Msg("Successfully redeployed Caddy instance")
		}
	}
}
