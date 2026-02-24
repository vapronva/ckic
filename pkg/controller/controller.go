package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/aggregator"
	"git.horse/vapronva/ckic/pkg/caddy"
	"git.horse/vapronva/ckic/pkg/constants"
	"git.horse/vapronva/ckic/pkg/handlers"
	"git.horse/vapronva/ckic/pkg/state"
	"git.horse/vapronva/ckic/pkg/utils"
	"git.horse/vapronva/ckic/pkg/watcher"
)

type ControllerConfig struct {
	Kubeconfig                   string
	NodeLabel                    string
	ConfigMapName                string
	ConfigMapNamespace           string
	BootstrapDefaultConfig       bool
	CommunicationMethod          caddy.CommunicationMethod
	CaddyImage                   string
	EnableLoadBalancer           bool
	PreferSavedState             bool
	EnvSecretName                string
	EnvSecretKeys                []string
	DataVolumePVC                string
	ConfigVolumePVC              string
	ExternalEndpoints            utils.ExternalEndpointsMap
	UseHostNetwork               bool
	CaddyAdminOriginKey          string
	HTTPHostPort                 int
	HTTPSHostPort                int
	ExternalEnable               bool
	ExternalLabel                string
	ExternalNsMode               string
	ExternalAllowNamespaces      string
	ExternalDenyNamespaces       string
	ExternalPublishAggregated    bool
	ExternalAggregatedConfigName string
}

type Controller struct {
	clientset         kubernetes.Interface
	config            ControllerConfig
	nodeWatcher       *watcher.NodeWatcher
	configWatcher     *watcher.ConfigWatcher
	externalWatcher   *watcher.ExternalConfigWatcher
	nodeHandler       *handlers.NodeHandler
	configHandler     *handlers.ConfigHandler
	deployedInstances map[string]*caddy.Instance
	instancesMutex    *sync.RWMutex
	stateStore        *state.ConfigMapStateStore
	coordinator       *WatcherCoordinator
	aggregator        any
}

const (
	nodeHandlerWorkerCount           = 4
	configReconciliationInterval     = 5 * time.Minute
	periodicStatePersistenceInterval = 2 * time.Minute
)

type watcherBundle struct {
	configWatcher   *watcher.ConfigWatcher
	externalWatcher *watcher.ExternalConfigWatcher
	agg             *aggregator.NamespaceAggregator
}

func NewController(clientset kubernetes.Interface, config ControllerConfig) (*Controller, error) {
	deployedInstances := make(map[string]*caddy.Instance)
	mutex := &sync.RWMutex{}
	nodeAvailabilityCheck := func() bool {
		mutex.RLock()
		defer mutex.RUnlock()
		return len(deployedInstances) > 0
	}
	nodeHandler := handlers.NewNodeHandler(
		clientset,
		config.ConfigMapNamespace,
		config.CaddyImage,
		config.EnableLoadBalancer,
		deployedInstances,
		mutex,
		nil,
		config.EnvSecretName,
		config.EnvSecretKeys,
		config.DataVolumePVC,
		config.ConfigVolumePVC,
		config.ConfigMapName,
		config.ExternalEndpoints,
		config.UseHostNetwork,
		config.HTTPHostPort,
		config.HTTPSHostPort,
	)
	nodeWatcher := watcher.NewNodeWatcher(clientset, config.NodeLabel, nodeHandler.Handle)
	configHandler := handlers.NewConfigHandler(
		config.CommunicationMethod,
		clientset,
		config.ConfigMapNamespace,
		config.CaddyImage,
		config.EnableLoadBalancer,
		deployedInstances,
		mutex,
		config.EnvSecretName,
		config.EnvSecretKeys,
		config.DataVolumePVC,
		config.ConfigVolumePVC,
		config.ConfigMapName,
		config.ExternalEndpoints,
		config.UseHostNetwork,
		config.CaddyAdminOriginKey,
		config.HTTPHostPort,
		config.HTTPSHostPort,
	)
	watchers := buildWatcherBundle(clientset, config, configHandler, nodeAvailabilityCheck)
	coordinator := NewWatcherCoordinator(
		nodeWatcher,
		watchers.configWatcher,
		deployedInstances,
		mutex,
	)
	nodeHandler.SetNodeChangeNotifier(coordinator.NotifyNodeChange)
	stateStore := state.NewConfigMapStateStore(
		clientset,
		config.ConfigMapNamespace,
		constants.StateConfigMapName,
	)
	ctrl := &Controller{
		clientset:         clientset,
		config:            config,
		nodeWatcher:       nodeWatcher,
		configWatcher:     watchers.configWatcher,
		externalWatcher:   watchers.externalWatcher,
		nodeHandler:       nodeHandler,
		configHandler:     configHandler,
		deployedInstances: deployedInstances,
		instancesMutex:    mutex,
		stateStore:        stateStore,
		coordinator:       coordinator,
	}
	if watchers.agg != nil {
		ctrl.aggregator = watchers.agg
	}
	return ctrl, nil
}

func buildWatcherBundle(
	clientset kubernetes.Interface,
	config ControllerConfig,
	configHandler *handlers.ConfigHandler,
	nodeAvailabilityCheck func() bool,
) watcherBundle {
	bundle := watcherBundle{}
	if !config.ExternalEnable {
		bundle.configWatcher = watcher.NewConfigWatcher(
			clientset,
			config.ConfigMapNamespace,
			config.ConfigMapName,
			configHandler.Handle,
			nodeAvailabilityCheck,
			config.BootstrapDefaultConfig,
		)
		return bundle
	}
	bundle.agg = aggregator.NewNamespaceAggregator(
		clientset,
		config.ConfigMapNamespace,
		config.ExternalPublishAggregated,
		config.ExternalAggregatedConfigName,
		configHandler.Handle,
		nodeAvailabilityCheck,
	)
	bundle.configWatcher = watcher.NewConfigWatcher(
		clientset,
		config.ConfigMapNamespace,
		config.ConfigMapName,
		bundle.agg.UpdateBase,
		nodeAvailabilityCheck,
		config.BootstrapDefaultConfig,
	)
	bundle.configWatcher.SetForceSyncHandler(bundle.agg.EnsureNodeSync)
	bundle.externalWatcher = watcher.NewExternalConfigWatcher(
		clientset,
		config.ConfigMapNamespace,
		config.ConfigMapName,
		config.ExternalLabel,
		config.ExternalNsMode,
		config.ExternalAllowNamespaces,
		config.ExternalDenyNamespaces,
		bundle.agg.SetExternal,
		bundle.agg.RemoveExternal,
	)
	return bundle
}

func (c *Controller) Run(ctx context.Context) error {
	if err := c.ReconcileState(ctx); err != nil {
		log.Error().Err(err).Msg("Reconciliation failed")
		return err
	}
	c.nodeHandler.StartWorkerPool(ctx, nodeHandlerWorkerCount)
	go c.nodeWatcher.Start(ctx)
	go c.configWatcher.Start(ctx)
	if c.config.ExternalEnable && c.externalWatcher != nil {
		log.Info().Msg("Starting external ConfigMap watcher")
		go c.externalWatcher.Start(ctx)
	}
	go c.runPeriodicConfigReconciliation(ctx)
	go c.runPeriodicStatePersistence(ctx)
	<-ctx.Done()
	log.Info().Msg("Controller shutting down")
	c.persistStateOnShutdown()
	return nil
}

//nolint:gocognit,nestif,cyclop,funlen
func (c *Controller) ReconcileState(ctx context.Context) error {
	logger := log.With().Str("component", "reconcile").Logger()
	logger.Info().Msg("Starting reconciliation process")
	discovered := make(map[string]*caddy.Instance)
	nodeLabelKey, nodeLabelValue := watcher.ParseLabelSelector(c.config.NodeLabel)
	if nodeLabelKey == "" {
		return errors.New("node label selector is empty")
	}
	nodeSelector := nodeLabelKey + "=" + nodeLabelValue
	nodes, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: nodeSelector,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to list nodes for reconciliation selector %q: %w",
			nodeSelector,
			err,
		)
	}
	eligibleNodes := make(map[string]struct{}, len(nodes.Items))
	for _, node := range nodes.Items {
		eligibleNodes[node.Name] = struct{}{}
	}
	deployments, err := c.clientset.AppsV1().Deployments(c.config.ConfigMapNamespace).List(
		ctx, metav1.ListOptions{
			LabelSelector: "ckic.cmld.ru/caddy-managed=true",
		},
	)
	if err != nil {
		return fmt.Errorf("failed to list deployments for reconciliation: %w", err)
	}
	for _, dep := range deployments.Items {
		nodeName, ok := dep.Labels["instance"]
		if !ok || nodeName == "" {
			logger.Warn().Msg("Deployment missing instance label, skipping")
			continue
		}
		if _, matchesSelector := eligibleNodes[nodeName]; !matchesSelector {
			logger.Debug().
				Str("node", nodeName).
				Str("selector", nodeSelector).
				Msg("Skipping managed deployment outside NodeLabel selector scope")
			continue
		}
		if dep.Status.ReadyReplicas > 0 {
			podList, errPL := c.clientset.CoreV1().
				Pods(c.config.ConfigMapNamespace).
				List(ctx, metav1.ListOptions{
					LabelSelector: "app=caddy,instance=" + nodeName,
				})
			if errPL != nil || len(podList.Items) == 0 {
				logger.Warn().Msgf("No pods found for healthy deployment on node %s", nodeName)
				continue
			}
			instance := &caddy.Instance{
				NodeName:       nodeName,
				Namespace:      c.config.ConfigMapNamespace,
				DeploymentName: dep.Name,
				ServiceName:    dep.Name,
				PodName:        podList.Items[0].Name,
				KubeClient:     c.clientset,
			}
			discovered[nodeName] = instance
			logger.Info().Msgf("Adopted existing healthy deployment on node %s", nodeName)
		} else {
			logger.Warn().
				Msgf("Deployment on node %s is not healthy, deleting orphaned deployment", nodeName)
			instance := &caddy.Instance{
				NodeName:       nodeName,
				Namespace:      c.config.ConfigMapNamespace,
				DeploymentName: dep.Name,
				ServiceName:    dep.Name,
				KubeClient:     c.clientset,
			}
			if errID := instance.Delete(); errID != nil {
				logger.Error().
					Err(errID).
					Msgf("Failed to delete orphaned deployment on node %s", nodeName)
			} else {
				logger.Info().Msgf("Deleted orphaned deployment on node %s", nodeName)
			}
		}
	}
	c.reconcileDiscoveredDeployments(ctx, discovered)
	savedState, err := c.stateStore.LoadState()
	if err != nil {
		logger.Warn().Err(err).Msg("Could not load saved state, proceeding with discovered state")
	}
	if c.config.PreferSavedState && len(savedState) > 0 {
		logger.Info().
			Msg("PreferSavedState is enabled. Merging saved state with discovered state, preferring saved state")
		for node := range discovered {
			if savedInst, exists := savedState[node]; exists {
				discovered[node].FailureCount.Store(savedInst.FailureCount.Load())
				logger.Debug().
					Str("node", node).
					Int32("failureCount", savedInst.FailureCount.Load()).
					Msg("Restored FailureCount from saved state")
			}
		}
	} else {
		logger.Info().Msg("Using discovered state")
	}
	c.instancesMutex.Lock()
	for node := range c.deployedInstances {
		delete(c.deployedInstances, node)
	}
	maps.Copy(c.deployedInstances, discovered)
	instancesCopy := make(map[string]*caddy.Instance, len(c.deployedInstances))
	maps.Copy(instancesCopy, c.deployedInstances)
	c.instancesMutex.Unlock()
	if saveErr := c.stateStore.SaveState(instancesCopy); saveErr != nil {
		logger.Error().Err(saveErr).Msg("Failed to persist state")
	}
	if c.config.ExternalEnable {
		if c.aggregator == nil {
			logger.Error().
				Msg("External aggregation enabled but aggregator is nil, skipping initial config setup")
		} else if agg, ok := c.aggregator.(*aggregator.NamespaceAggregator); ok {
			if c.externalWatcher != nil {
				externals, errBatch := c.externalWatcher.InitialListBatch(ctx)
				if errBatch != nil {
					logger.Error().Err(errBatch).Msg("Failed to batch load external ConfigMaps")
				} else if len(externals) > 0 {
					agg.SetExternalBatch(externals)
				}
			}
			configMap, configMapErr := c.clientset.CoreV1().
				ConfigMaps(c.config.ConfigMapNamespace).
				Get(ctx, c.config.ConfigMapName, metav1.GetOptions{})
			if configMapErr != nil {
				logger.Error().
					Err(configMapErr).
					Msg("Failed to get ConfigMap for initial config push")
			} else if configData, exists := configMap.Data["Caddyfile"]; exists {
				agg.UpdateBase(configData)
			} else {
				logger.Warn().Msg("ConfigMap missing Caddyfile key, skipping base config setup")
			}
			agg.MarkInitialized()
			if len(c.deployedInstances) > 0 {
				logger.Info().Msg("Initial configuration pushed to discovered instances")
			} else {
				logger.Info().Msg("Aggregator initialized (no instances to push to yet)")
			}
		} else {
			logger.Error().Msg("Failed to assert aggregator type, skipping initial config setup")
		}
	} else if len(c.deployedInstances) > 0 {
		logger.Info().Msg("Pushing initial configuration to discovered instances")
		configMap, configMapErr := c.clientset.CoreV1().ConfigMaps(c.config.ConfigMapNamespace).Get(
			ctx, c.config.ConfigMapName, metav1.GetOptions{})
		if configMapErr != nil {
			logger.Error().Err(configMapErr).Msg("Failed to get ConfigMap for initial config push")
		} else if configData, exists := configMap.Data["Caddyfile"]; exists {
			c.configHandler.Handle(configData)
		} else {
			logger.Warn().Msg("ConfigMap missing Caddyfile key, skipping initial config push")
		}
	}
	logger.Info().Msg("Reconciliation process completed")
	return nil
}

func (c *Controller) reconcileDiscoveredDeployments(
	ctx context.Context,
	discovered map[string]*caddy.Instance,
) {
	logger := log.With().Str("component", "reconcile").Logger()
	for nodeName, adopted := range discovered {
		reconciled, err := caddy.DeployCaddy(
			ctx,
			c.clientset,
			nodeName,
			c.config.ConfigMapNamespace,
			c.config.CaddyImage,
			c.config.EnableLoadBalancer,
			c.config.ExternalEndpoints[nodeName],
			c.config.EnvSecretName,
			c.config.EnvSecretKeys,
			c.config.DataVolumePVC,
			c.config.ConfigVolumePVC,
			c.config.ConfigMapName,
			c.config.UseHostNetwork,
			c.config.HTTPHostPort,
			c.config.HTTPSHostPort,
		)
		if err != nil {
			logger.Error().
				Err(err).
				Str("node", nodeName).
				Msg("Failed to reconcile adopted deployment, keeping discovered instance")
			continue
		}
		discovered[nodeName] = reconciled
		if adopted == nil || adopted.PodName != reconciled.PodName {
			logger.Info().Msgf("Reconciled adopted deployment on node %s", nodeName)
			continue
		}
		logger.Debug().Str("node", nodeName).Msg("Adopted deployment already matches desired spec")
	}
}

//nolint:gocognit
func (c *Controller) runPeriodicConfigReconciliation(ctx context.Context) {
	ticker := time.NewTicker(configReconciliationInterval)
	defer ticker.Stop()
	logger := log.With().Str("component", "config_reconciliation").Logger()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			logger.Info().Msg("Performing periodic configuration reconciliation")
			configMap, err := c.clientset.CoreV1().ConfigMaps(c.config.ConfigMapNamespace).Get(
				ctx, c.config.ConfigMapName, metav1.GetOptions{})
			if err != nil {
				logger.Error().Err(err).Msg("Failed to get ConfigMap during reconciliation")
				continue
			}
			configData, exists := configMap.Data["Caddyfile"]
			if !exists {
				logger.Warn().Msg("ConfigMap missing Caddyfile key during reconciliation")
				continue
			}
			if c.config.ExternalEnable {
				if c.aggregator == nil {
					logger.Error().
						Msg("External aggregation enabled but aggregator is nil, skipping reconciliation to prevent configuration drift")
					continue
				}
				if agg, ok := c.aggregator.(*aggregator.NamespaceAggregator); ok {
					agg.UpdateBase(configData)
				} else {
					logger.Error().
						Msg("Failed to assert aggregator type, skipping reconciliation to prevent configuration drift")
					continue
				}
			} else {
				c.configHandler.Handle(configData)
			}
			logger.Info().Msg("Periodic configuration reconciliation completed")
		}
	}
}

func (c *Controller) runPeriodicStatePersistence(ctx context.Context) {
	ticker := time.NewTicker(periodicStatePersistenceInterval)
	defer ticker.Stop()
	logger := log.With().Str("component", "state_persistence").Logger()
	logger.Info().Msg("Starting periodic state persistence (every 2 minutes)")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.instancesMutex.RLock()
			instanceCount := len(c.deployedInstances)
			if instanceCount == 0 {
				c.instancesMutex.RUnlock()
				logger.Debug().Msg("No deployed instances, skipping state persistence")
				continue
			}
			instancesCopy := make(map[string]*caddy.Instance, instanceCount)
			maps.Copy(instancesCopy, c.deployedInstances)
			c.instancesMutex.RUnlock()
			if err := c.stateStore.SaveState(instancesCopy); err != nil {
				logger.Error().Err(err).Msg("Failed to persist state during periodic save")
			} else {
				logger.Debug().
					Int("instances", instanceCount).
					Msg("Periodic state persistence completed")
			}
		}
	}
}

func (c *Controller) persistStateOnShutdown() {
	logger := log.With().Str("component", "state_persistence").Logger()
	logger.Info().Msg("Persisting state before shutdown")
	c.instancesMutex.RLock()
	instanceCount := len(c.deployedInstances)
	if instanceCount == 0 {
		c.instancesMutex.RUnlock()
		logger.Info().Msg("No deployed instances, skipping shutdown state persistence")
		return
	}
	instancesCopy := make(map[string]*caddy.Instance, instanceCount)
	maps.Copy(instancesCopy, c.deployedInstances)
	c.instancesMutex.RUnlock()
	if err := c.stateStore.SaveState(instancesCopy); err != nil {
		logger.Error().Err(err).Msg("Failed to persist state during shutdown")
	} else {
		logger.Info().
			Int("instances", instanceCount).
			Msg("State persisted successfully before shutdown")
	}
}
