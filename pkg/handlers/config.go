package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	Clientset           kubernetes.Interface
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
	lastConfigDigest    string
	instanceDigests     map[string]string
	instanceSignatures  map[string]string
}

type failedInstance struct {
	nodeName string
	instance *caddy.Instance
}

const (
	configUpdateConcurrency = 5
	configUpdateRetryCount  = 3
	configUpdateRetryFactor = 2
	maxInstanceFailureCount = 5
)

func NewConfigHandler(
	method caddy.CommunicationMethod,
	clientset kubernetes.Interface,
	namespace, caddyImage string,
	enableLoadBalancer bool,
	instances map[string]*caddy.Instance,
	mu *sync.RWMutex,
	envSecretName string,
	envSecretKeys []string,
	dataVolumePVC, configVolumePVC, configMapName string,
	externalEndpoints utils.ExternalEndpointsMap,
	useHostNetwork bool,
	caddyAdminOriginKey string,
	httpHostPort, httpsHostPort int,
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
		instanceDigests:     make(map[string]string),
		instanceSignatures:  make(map[string]string),
	}
}

//nolint:gocognit,nestif,funlen,cyclop
func (h *ConfigHandler) Handle(configData string) {
	h.handleMu.Lock()
	defer h.handleMu.Unlock()
	if h.instanceDigests == nil {
		h.instanceDigests = make(map[string]string)
	}
	if h.instanceSignatures == nil {
		h.instanceSignatures = make(map[string]string)
	}
	logger := log.With().Str("component", "config_handler").Logger()
	configDigest := calculateConfigDigest(configData)
	logger.Info().Msg("Detected configuration change, updating Caddy instances")
	h.Mu.RLock()
	instancesCopy := make(map[string]*caddy.Instance)
	maps.Copy(instancesCopy, h.DeployedInstances)
	h.Mu.RUnlock()
	for nodeName := range h.instanceDigests {
		if _, exists := instancesCopy[nodeName]; !exists {
			delete(h.instanceDigests, nodeName)
		}
	}
	for nodeName := range h.instanceSignatures {
		if _, exists := instancesCopy[nodeName]; !exists {
			delete(h.instanceSignatures, nodeName)
		}
	}
	if len(instancesCopy) == 0 {
		h.lastConfigDigest = configDigest
		logger.Debug().
			Str("configDigest", configDigest).
			Msg("No deployed instances, skipping config push")
		return
	}
	if configDigest == h.lastConfigDigest && h.allInstancesAtDigest(instancesCopy, configDigest) {
		logger.Info().
			Str("configDigest", configDigest).
			Msg("Configuration digest unchanged across all instances, skipping update")
		return
	}
	time.Sleep(constants.ConfigUpdateDelay)
	var wg sync.WaitGroup
	var muFailed sync.Mutex
	var muSuccess sync.Mutex
	var failedInstances []failedInstance
	var successfulNodes []string
	semaphore := make(chan struct{}, configUpdateConcurrency)
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
			for retry := range configUpdateRetryCount {
				if retry > 0 {
					instanceLogger.Info().Int("retry", retry).Msg("Retrying configuration update")
					time.Sleep(time.Duration(retry*configUpdateRetryFactor) * time.Second)
				}
				err = instance.UpdateConfig(
					context.Background(),
					configData,
					h.CommunicationMethod,
					apiConfig,
				)
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
					instanceLogger.Debug().
						Msg("Instance replaced during update, skipping failure tracking")
					return
				}
				if newCount >= maxInstanceFailureCount {
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
					instanceLogger.Debug().
						Msg("Instance replaced during update, skipping failure reset")
					return
				}
				muSuccess.Lock()
				successfulNodes = append(successfulNodes, nodeName)
				muSuccess.Unlock()
			}
		}(nodeName, instance)
	}
	wg.Wait()
	for _, nodeName := range successfulNodes {
		h.instanceDigests[nodeName] = configDigest
		if instance, exists := instancesCopy[nodeName]; exists {
			h.instanceSignatures[nodeName] = instanceStateSignature(instance)
		}
	}
	if len(failedInstances) > 0 {
		logger.Info().Msgf("Redeploying %d failed instances", len(failedInstances))
		for _, failed := range failedInstances {
			h.Mu.Lock()
			current := h.DeployedInstances[failed.nodeName]
			if current == nil {
				h.Mu.Unlock()
				logger.Debug().
					Str("node", failed.nodeName).
					Msg("Failed instance no longer exists, skipping redeployment")
				continue
			}
			if current != failed.instance {
				h.Mu.Unlock()
				logger.Debug().
					Str("node", failed.nodeName).
					Msg("Failed instance was replaced, skipping redeployment")
				continue
			}
			delete(h.DeployedInstances, failed.nodeName)
			h.Mu.Unlock()
			logger.Info().Str("node", failed.nodeName).Msg("Redeploying failed Caddy instance")
			if err := failed.instance.Delete(); err != nil {
				logger.Error().
					Err(err).
					Str("node", failed.nodeName).
					Msg("Failed to delete failed Caddy instance")
				h.Mu.Lock()
				if _, exists := h.DeployedInstances[failed.nodeName]; !exists {
					h.DeployedInstances[failed.nodeName] = failed.instance
				}
				h.Mu.Unlock()
				delete(h.instanceDigests, failed.nodeName)
				delete(h.instanceSignatures, failed.nodeName)
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
				logger.Error().
					Err(err).
					Str("node", failed.nodeName).
					Msg("Failed to redeploy Caddy instance")
				h.Mu.Lock()
				if _, exists := h.DeployedInstances[failed.nodeName]; !exists {
					h.DeployedInstances[failed.nodeName] = failed.instance
				}
				h.Mu.Unlock()
				delete(h.instanceDigests, failed.nodeName)
				delete(h.instanceSignatures, failed.nodeName)
				continue
			}
			replaced := false
			h.Mu.Lock()
			if _, exists := h.DeployedInstances[failed.nodeName]; !exists {
				h.DeployedInstances[failed.nodeName] = newInstance
				replaced = true
			}
			h.Mu.Unlock()
			if !replaced {
				logger.Debug().
					Str("node", failed.nodeName).
					Msg("Failed instance was replaced during redeployment; cleaning up stale redeploy")
				if cleanupErr := newInstance.Delete(); cleanupErr != nil {
					logger.Warn().
						Err(cleanupErr).
						Str("node", failed.nodeName).
						Msg("Failed to clean up stale redeployed Caddy instance")
				}
				delete(h.instanceDigests, failed.nodeName)
				delete(h.instanceSignatures, failed.nodeName)
				continue
			}
			h.instanceDigests[failed.nodeName] = configDigest
			h.instanceSignatures[failed.nodeName] = instanceStateSignature(newInstance)
			logger.Info().Str("node", failed.nodeName).Msg("Successfully redeployed Caddy instance")
		}
	}
	if h.allCurrentInstancesConverged(configDigest) {
		h.lastConfigDigest = configDigest
	}
}

func (h *ConfigHandler) allInstancesAtDigest(
	instances map[string]*caddy.Instance,
	configDigest string,
) bool {
	for nodeName, instance := range instances {
		if h.instanceDigests[nodeName] != configDigest {
			return false
		}
		if h.instanceSignatures[nodeName] != instanceStateSignature(instance) {
			return false
		}
	}
	return true
}

func (h *ConfigHandler) allCurrentInstancesConverged(configDigest string) bool {
	h.Mu.RLock()
	defer h.Mu.RUnlock()
	for nodeName, instance := range h.DeployedInstances {
		if h.instanceDigests[nodeName] != configDigest {
			return false
		}
		if h.instanceSignatures[nodeName] != instanceStateSignature(instance) {
			return false
		}
	}
	return true
}

func instanceStateSignature(instance *caddy.Instance) string {
	if instance == nil {
		return ""
	}
	return instance.DeploymentName + "|" + instance.ServiceName + "|" + instance.PodName
}

func calculateConfigDigest(configData string) string {
	sum := sha256.Sum256([]byte(configData))
	return hex.EncodeToString(sum[:])
}
