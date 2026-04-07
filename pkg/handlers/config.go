package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"git.horse/vapronva/ckic/pkg/caddy"
	"git.horse/vapronva/ckic/pkg/utils"
)

type ConfigHandler struct {
	deployOpts          caddy.DeployOptions
	communicationMethod caddy.CommunicationMethod
	caddyAdminOriginKey string
	externalEndpoints   utils.ExternalEndpointsMap
	deployedInstances   map[string]*caddy.Instance
	mu                  *sync.RWMutex
	handleMu            sync.Mutex
	lastConfigDigest    string
	instanceDigests     map[string]string
	instanceSignatures  map[string]string
}

type failedInstance struct {
	nodeName string
	instance *caddy.Instance
}

type configUpdateResult struct {
	successfulNodes []string
	failedInstances []failedInstance
}

const (
	configUpdateDelay         = 1 * time.Second
	configUpdateConcurrency   = 5
	configUpdateRetryCount    = 3
	configUpdateRetryFactor   = 2
	maxInstanceFailureCount   = 5
	redeployConfigPushTimeout = 6 * time.Minute
)

func NewConfigHandler(
	deployOpts caddy.DeployOptions,
	method caddy.CommunicationMethod,
	caddyAdminOriginKey string,
	externalEndpoints utils.ExternalEndpointsMap,
	instances map[string]*caddy.Instance,
	mu *sync.RWMutex,
) *ConfigHandler {
	return &ConfigHandler{
		deployOpts:          deployOpts,
		communicationMethod: method,
		caddyAdminOriginKey: caddyAdminOriginKey,
		externalEndpoints:   externalEndpoints,
		deployedInstances:   instances,
		mu:                  mu,
		instanceDigests:     make(map[string]string),
		instanceSignatures:  make(map[string]string),
	}
}

func (h *ConfigHandler) Handle(configData string) {
	h.handleMu.Lock()
	defer h.handleMu.Unlock()
	logger := log.With().Str("component", "config_handler").Logger()
	configDigest := calculateConfigDigest(configData)
	logger.Info().Msg("Detected configuration change, updating Caddy instances")
	instancesCopy := h.snapshotInstances()
	h.pruneTrackingMaps(instancesCopy)
	if len(instancesCopy) == 0 {
		h.lastConfigDigest = configDigest
		logger.Debug().
			Str("configDigest", configDigest).
			Msg("No deployed instances, skipping config push")
		return
	}
	instancesToUpdate := make(map[string]*caddy.Instance)
	for nodeName, instance := range instancesCopy {
		if h.instanceDigests[nodeName] != configDigest ||
			h.instanceSignatures[nodeName] != instanceStateSignature(instance) {
			instancesToUpdate[nodeName] = instance
		}
	}
	if len(instancesToUpdate) == 0 {
		logger.Info().
			Str("configDigest", configDigest).
			Msg("All instances already at desired digest, skipping update")
		h.lastConfigDigest = configDigest
		return
	}
	time.Sleep(configUpdateDelay)
	updateResult := h.updateAllInstances(
		instancesToUpdate,
		configData,
		h.adminAPIConfig(),
		logger,
	)
	h.applySuccessfulUpdates(updateResult.successfulNodes, instancesCopy, configDigest)
	h.redeployFailedInstances(
		updateResult.failedInstances,
		configData,
		configDigest,
		logger,
	)
	if h.allCurrentInstancesConverged(configDigest) {
		h.lastConfigDigest = configDigest
	}
}

func (h *ConfigHandler) snapshotInstances() map[string]*caddy.Instance {
	h.mu.RLock()
	defer h.mu.RUnlock()
	instancesCopy := make(map[string]*caddy.Instance, len(h.deployedInstances))
	maps.Copy(instancesCopy, h.deployedInstances)
	return instancesCopy
}

func (h *ConfigHandler) pruneTrackingMaps(instancesCopy map[string]*caddy.Instance) {
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
}

func (h *ConfigHandler) adminAPIConfig() *caddy.AdminAPIConfig {
	if h.caddyAdminOriginKey == "" {
		return nil
	}
	return &caddy.AdminAPIConfig{OriginKey: h.caddyAdminOriginKey}
}

func (h *ConfigHandler) updateAllInstances(
	instancesCopy map[string]*caddy.Instance,
	effectiveConfig string,
	apiConfig *caddy.AdminAPIConfig,
	logger zerolog.Logger,
) configUpdateResult {
	var wg sync.WaitGroup
	var mu sync.Mutex
	result := configUpdateResult{
		successfulNodes: make([]string, 0, len(instancesCopy)),
		failedInstances: make([]failedInstance, 0),
	}
	semaphore := make(chan struct{}, configUpdateConcurrency)
	for nodeName, instance := range instancesCopy {
		wg.Add(1)
		go func(nodeName string, instance *caddy.Instance) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			if failed, successfulNode, hasFailure := h.updateSingleInstance(
				nodeName,
				instance,
				effectiveConfig,
				apiConfig,
				logger,
			); hasFailure {
				mu.Lock()
				result.failedInstances = append(result.failedInstances, failed)
				mu.Unlock()
			} else if successfulNode != "" {
				mu.Lock()
				result.successfulNodes = append(result.successfulNodes, successfulNode)
				mu.Unlock()
			}
		}(nodeName, instance)
	}
	wg.Wait()
	return result
}

func (h *ConfigHandler) updateSingleInstance(
	nodeName string,
	instance *caddy.Instance,
	effectiveConfig string,
	apiConfig *caddy.AdminAPIConfig,
	logger zerolog.Logger,
) (failedInstance, string, bool) {
	instanceLogger := logger.With().Str("node", nodeName).Logger()
	instanceLogger.Debug().Msg("Updating Caddy configuration")
	err := h.updateInstanceWithRetry(
		instance,
		effectiveConfig,
		apiConfig,
		instanceLogger,
	)
	if err != nil {
		return h.handleInstanceUpdateFailure(nodeName, instance, err, instanceLogger)
	}
	return h.handleInstanceUpdateSuccess(nodeName, instance, instanceLogger)
}

func (h *ConfigHandler) updateInstanceWithRetry(
	instance *caddy.Instance,
	effectiveConfig string,
	apiConfig *caddy.AdminAPIConfig,
	instanceLogger zerolog.Logger,
) error {
	var err error
	for retry := range configUpdateRetryCount {
		if retry > 0 {
			instanceLogger.Info().
				Int("retry", retry).
				Msg("Retrying configuration update")
			time.Sleep(time.Duration(retry*configUpdateRetryFactor) * time.Second)
		}
		err = instance.UpdateConfig(
			context.Background(),
			effectiveConfig,
			h.communicationMethod,
			apiConfig,
		)
		if err == nil {
			return nil
		}
	}
	return err
}

func (h *ConfigHandler) handleInstanceUpdateFailure(
	nodeName string,
	instance *caddy.Instance,
	updateErr error,
	instanceLogger zerolog.Logger,
) (failedInstance, string, bool) {
	instanceLogger.Error().
		Err(updateErr).
		Msg("Failed to update Caddy configuration")
	h.mu.RLock()
	current := h.deployedInstances[nodeName] == instance
	var newCount int32
	if current {
		newCount = instance.FailureCount.Add(1)
	}
	h.mu.RUnlock()
	if !current {
		instanceLogger.Debug().
			Msg("Instance replaced during update, skipping failure tracking")
		return failedInstance{}, "", false
	}
	if newCount < maxInstanceFailureCount {
		return failedInstance{}, "", false
	}
	instanceLogger.Warn().
		Msg("Too many update failures, marking for redeployment")
	return failedInstance{
		nodeName: nodeName,
		instance: instance,
	}, "", true
}

func (h *ConfigHandler) handleInstanceUpdateSuccess(
	nodeName string,
	instance *caddy.Instance,
	instanceLogger zerolog.Logger,
) (failedInstance, string, bool) {
	instanceLogger.Info().Msg("Successfully updated Caddy configuration")
	h.mu.RLock()
	current := h.deployedInstances[nodeName] == instance
	if current {
		instance.FailureCount.Store(0)
	}
	h.mu.RUnlock()
	if !current {
		instanceLogger.Debug().
			Msg("Instance replaced during update, skipping failure reset")
		return failedInstance{}, "", false
	}
	return failedInstance{}, nodeName, false
}

func (h *ConfigHandler) applySuccessfulUpdates(
	successfulNodes []string,
	instancesCopy map[string]*caddy.Instance,
	configDigest string,
) {
	for _, nodeName := range successfulNodes {
		h.instanceDigests[nodeName] = configDigest
		if instance, exists := instancesCopy[nodeName]; exists {
			h.instanceSignatures[nodeName] = instanceStateSignature(instance)
		}
	}
}

func (h *ConfigHandler) redeployFailedInstances(
	failedInstances []failedInstance,
	configData string,
	configDigest string,
	logger zerolog.Logger,
) {
	if len(failedInstances) == 0 {
		return
	}
	logger.Info().Msgf("Redeploying %d failed instances", len(failedInstances))
	for _, failed := range failedInstances {
		h.redeployFailedInstance(failed, configData, configDigest, logger)
	}
}

func (h *ConfigHandler) redeployFailedInstance(
	failed failedInstance,
	configData string,
	configDigest string,
	logger zerolog.Logger,
) {
	current, shouldRedeploy := h.prepareFailedInstanceRedeploy(failed, logger)
	if !shouldRedeploy {
		return
	}
	if deleteErr := failed.instance.Delete(context.Background()); deleteErr != nil {
		logger.Error().
			Err(deleteErr).
			Str("node", failed.nodeName).
			Msg("Failed to delete failed Caddy instance")
		restoreInstanceIfMissing(h.mu, h.deployedInstances, failed.nodeName, current)
		h.clearInstanceTracking(failed.nodeName)
		return
	}
	newInstance, redeployErr := h.deployReplacementInstance(failed.nodeName)
	if redeployErr != nil {
		logger.Error().
			Err(redeployErr).
			Str("node", failed.nodeName).
			Msg("Failed to redeploy Caddy instance")
		restoreInstanceIfMissing(h.mu, h.deployedInstances, failed.nodeName, current)
		h.clearInstanceTracking(failed.nodeName)
		return
	}
	if !h.registerReplacementInstance(failed.nodeName, newInstance) {
		logger.Debug().
			Str("node", failed.nodeName).
			Msg("Failed instance was replaced during redeployment; cleaning up stale redeploy")
		if cleanupErr := newInstance.Delete(context.Background()); cleanupErr != nil {
			logger.Warn().
				Err(cleanupErr).
				Str("node", failed.nodeName).
				Msg("Failed to clean up stale redeployed Caddy instance")
		}
		h.clearInstanceTracking(failed.nodeName)
		return
	}
	pushCtx, pushCancel := context.WithTimeout(
		context.Background(), redeployConfigPushTimeout,
	)
	defer pushCancel()
	if pushErr := newInstance.UpdateConfig(
		pushCtx,
		configData,
		h.communicationMethod,
		h.adminAPIConfig(),
	); pushErr != nil {
		logger.Error().
			Err(pushErr).
			Str("node", failed.nodeName).
			Msg("Failed to push config to redeployed instance")
		h.clearInstanceTracking(failed.nodeName)
		return
	}
	h.instanceDigests[failed.nodeName] = configDigest
	h.instanceSignatures[failed.nodeName] = instanceStateSignature(newInstance)
	logger.Info().
		Str("node", failed.nodeName).
		Msg("Successfully redeployed Caddy instance")
}

func (h *ConfigHandler) prepareFailedInstanceRedeploy(
	failed failedInstance,
	logger zerolog.Logger,
) (*caddy.Instance, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	current := h.deployedInstances[failed.nodeName]
	if current == nil {
		logger.Debug().
			Str("node", failed.nodeName).
			Msg("Failed instance no longer exists, skipping redeployment")
		return nil, false
	}
	if current != failed.instance {
		logger.Debug().
			Str("node", failed.nodeName).
			Msg("Failed instance was replaced, skipping redeployment")
		return nil, false
	}
	delete(h.deployedInstances, failed.nodeName)
	logger.Info().
		Str("node", failed.nodeName).
		Msg("Redeploying failed Caddy instance")
	return current, true
}

func (h *ConfigHandler) clearInstanceTracking(nodeName string) {
	delete(h.instanceDigests, nodeName)
	delete(h.instanceSignatures, nodeName)
}

func (h *ConfigHandler) deployReplacementInstance(
	nodeName string,
) (*caddy.Instance, error) {
	return caddy.DeployCaddy(
		context.Background(),
		h.deployOpts,
		nodeName,
		h.externalEndpoints[nodeName],
	)
}

func (h *ConfigHandler) registerReplacementInstance(
	nodeName string,
	newInstance *caddy.Instance,
) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.deployedInstances[nodeName]; exists {
		return false
	}
	h.deployedInstances[nodeName] = newInstance
	return true
}

func (h *ConfigHandler) allCurrentInstancesConverged(configDigest string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for nodeName, instance := range h.deployedInstances {
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
	return instance.StateKey()
}

func calculateConfigDigest(configData string) string {
	sum := sha256.Sum256([]byte(configData))
	return hex.EncodeToString(sum[:])
}
