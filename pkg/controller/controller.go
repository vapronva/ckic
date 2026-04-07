package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/rs/zerolog"
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
	clientset              kubernetes.Interface
	config                 ControllerConfig
	deployOpts             caddy.DeployOptions
	nodeWatcher            *watcher.NodeWatcher
	configWatcher          *watcher.ConfigWatcher
	externalWatcher        *watcher.ExternalConfigWatcher
	nodeHandler            *handlers.NodeHandler
	configHandler          *handlers.ConfigHandler
	deployedInstances      map[string]*caddy.Instance
	instancesMutex         *sync.RWMutex
	stateStore             *state.ConfigMapStateStore
	coordinator            *WatcherCoordinator
	aggregator             *aggregator.NamespaceAggregator
	lastPersistedStateHash [sha256.Size]byte
}

const (
	nodeHandlerWorkerCount           = 4
	configReconciliationInterval     = 5 * time.Minute
	periodicStatePersistenceInterval = 2 * time.Minute
)

var errConfigMapMissingCaddyfile = errors.New("configmap missing Caddyfile key")

type watcherBundle struct {
	configWatcher   *watcher.ConfigWatcher
	externalWatcher *watcher.ExternalConfigWatcher
	agg             *aggregator.NamespaceAggregator
}

func NewController(
	clientset kubernetes.Interface,
	config ControllerConfig,
) (*Controller, error) {
	deployedInstances, mutex, nodeAvailabilityCheck := newControllerState()
	deployOpts := caddy.DeployOptions{
		Clientset:          clientset,
		Namespace:          config.ConfigMapNamespace,
		CaddyImage:         config.CaddyImage,
		EnableLoadBalancer: config.EnableLoadBalancer,
		EnvSecretName:      config.EnvSecretName,
		EnvSecretKeys:      config.EnvSecretKeys,
		DataVolumePVC:      config.DataVolumePVC,
		ConfigVolumePVC:    config.ConfigVolumePVC,
		ConfigMapName:      config.ConfigMapName,
		UseHostNetwork:     config.UseHostNetwork,
		HTTPHostPort:       config.HTTPHostPort,
		HTTPSHostPort:      config.HTTPSHostPort,
	}
	nodeHandler := handlers.NewNodeHandler(
		deployOpts,
		deployedInstances,
		mutex,
		config.ExternalEndpoints,
		nil,
	)
	nodeWatcher, err := watcher.NewNodeWatcher(
		clientset,
		config.NodeLabel,
		nodeHandler.Handle,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create node watcher: %w", err)
	}
	configHandler := handlers.NewConfigHandler(
		deployOpts,
		config.CommunicationMethod,
		config.CaddyAdminOriginKey,
		config.ExternalEndpoints,
		deployedInstances,
		mutex,
	)
	watchers := buildWatcherBundle(
		clientset,
		config,
		configHandler,
		nodeAvailabilityCheck,
	)
	coordinator := NewWatcherCoordinator(
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
		deployOpts:        deployOpts,
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
	ctrl.aggregator = watchers.agg
	return ctrl, nil
}

func newControllerState() (
	map[string]*caddy.Instance,
	*sync.RWMutex,
	func() bool,
) {
	deployedInstances := make(map[string]*caddy.Instance)
	mutex := &sync.RWMutex{}
	nodeAvailabilityCheck := func() bool {
		mutex.RLock()
		defer mutex.RUnlock()
		return len(deployedInstances) > 0
	}
	return deployedInstances, mutex, nodeAvailabilityCheck
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

func (c *Controller) ReconcileState(ctx context.Context) error {
	logger := log.With().Str("component", "reconcile").Logger()
	logger.Info().Msg("Starting reconciliation process")
	eligibleNodes, nodeSelector, err := c.listEligibleNodes(ctx)
	if err != nil {
		return err
	}
	discovered, err := c.discoverManagedDeployments(
		ctx,
		eligibleNodes,
		nodeSelector,
		logger,
	)
	if err != nil {
		return err
	}
	c.reconcileDiscoveredDeployments(ctx, discovered)
	c.mergeSavedState(ctx, discovered, logger)
	c.replaceAndPersistState(ctx, discovered, logger)
	c.initializeConfiguration(ctx, logger)
	logger.Info().Msg("Reconciliation process completed")
	return nil
}

func (c *Controller) listEligibleNodes(
	ctx context.Context,
) (map[string]struct{}, string, error) {
	nodeSelector, err := watcher.NormalizeNodeLabelSelector(c.config.NodeLabel)
	if err != nil {
		return nil, "", fmt.Errorf(
			"invalid node label selector for reconciliation: %w",
			err,
		)
	}
	nodes, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: nodeSelector,
	})
	if err != nil {
		return nil, "", fmt.Errorf(
			"failed to list nodes for reconciliation selector %q: %w",
			nodeSelector,
			err,
		)
	}
	eligibleNodes := make(map[string]struct{}, len(nodes.Items))
	for _, node := range nodes.Items {
		eligibleNodes[node.Name] = struct{}{}
	}
	return eligibleNodes, nodeSelector, nil
}

func (c *Controller) discoverManagedDeployments(
	ctx context.Context,
	eligibleNodes map[string]struct{},
	nodeSelector string,
	logger zerolog.Logger,
) (map[string]*caddy.Instance, error) {
	discovered := make(map[string]*caddy.Instance)
	deployments, err := c.clientset.AppsV1().
		Deployments(c.config.ConfigMapNamespace).
		List(ctx, metav1.ListOptions{
			LabelSelector: constants.ManagedLabelSelector,
		})
	if err != nil {
		return nil, fmt.Errorf("failed to list deployments for reconciliation: %w", err)
	}
	for _, dep := range deployments.Items {
		nodeName, ok := dep.Labels["instance"]
		if !ok || nodeName == "" {
			logger.Warn().Msg("Deployment missing instance label, skipping")
			continue
		}
		instance := &caddy.Instance{
			NodeName:       nodeName,
			Namespace:      c.config.ConfigMapNamespace,
			DeploymentName: dep.Name,
			ServiceName:    dep.Name,
			KubeClient:     c.clientset,
		}
		if _, matchesSelector := eligibleNodes[nodeName]; !matchesSelector {
			c.deleteOutOfScopeDeployment(instance, nodeName, nodeSelector, logger)
			continue
		}
		c.attachPodNameForAdoptedDeployment(ctx, nodeName, instance, logger)
		discovered[nodeName] = instance
		c.logAdoptedDeployment(dep.Status.ReadyReplicas > 0, nodeName, logger)
	}
	return discovered, nil
}

func (c *Controller) deleteOutOfScopeDeployment(
	instance *caddy.Instance,
	nodeName, nodeSelector string,
	logger zerolog.Logger,
) {
	logger.Warn().
		Str("node", nodeName).
		Str("selector", nodeSelector).
		Msg("Deleting managed deployment outside NodeLabel selector scope")
	if err := instance.Delete(context.Background()); err != nil {
		logger.Error().
			Err(err).
			Msgf("Failed to delete orphaned deployment on node %s", nodeName)
		return
	}
	logger.Info().Msgf("Deleted orphaned deployment on node %s", nodeName)
}

func (c *Controller) attachPodNameForAdoptedDeployment(
	ctx context.Context,
	nodeName string,
	instance *caddy.Instance,
	logger zerolog.Logger,
) {
	podList, err := c.clientset.CoreV1().
		Pods(c.config.ConfigMapNamespace).
		List(ctx, metav1.ListOptions{
			LabelSelector: "app=caddy,instance=" + nodeName,
		})
	if err != nil {
		logger.Warn().
			Err(err).
			Msgf("Failed to list pods while adopting deployment on node %s", nodeName)
		return
	}
	if len(podList.Items) == 0 {
		logger.Debug().
			Str("node", nodeName).
			Msg("No pods found yet for managed deployment; adopting transitional deployment")
		return
	}
	if podName, selected := caddy.SelectNewestActivePodName(podList.Items); selected {
		instance.PodName = podName
		return
	}
	logger.Debug().
		Str("node", nodeName).
		Msg("Only terminating pods found for managed deployment; adopting transitional deployment")
}

func (c *Controller) logAdoptedDeployment(
	healthy bool,
	nodeName string,
	logger zerolog.Logger,
) {
	if healthy {
		logger.Info().Msgf("Adopted existing healthy deployment on node %s", nodeName)
		return
	}
	logger.Info().Msgf("Adopted existing transitioning deployment on node %s", nodeName)
}

func (c *Controller) mergeSavedState(
	ctx context.Context,
	discovered map[string]*caddy.Instance,
	logger zerolog.Logger,
) {
	savedState, err := c.stateStore.LoadState(ctx)
	if err != nil {
		logger.Warn().
			Err(err).
			Msg("Could not load saved state, proceeding with discovered state")
	}
	for node := range discovered {
		savedInst, exists := savedState[node]
		if !exists || savedInst == nil {
			continue
		}
		if savedInst.FailureCount.Load() > 0 {
			discovered[node].FailureCount.Store(savedInst.FailureCount.Load())
			logger.Debug().
				Str("node", node).
				Int32("failureCount", savedInst.FailureCount.Load()).
				Msg("Restored FailureCount from saved state")
		}
	}
	if !c.config.PreferSavedState || len(savedState) == 0 {
		logger.Info().Msg("Using discovered state")
		return
	}
	logger.Info().
		Msg("PreferSavedState is enabled. Merging saved state with discovered state, preferring saved state")
}

func (c *Controller) replaceAndPersistState(
	ctx context.Context,
	discovered map[string]*caddy.Instance,
	logger zerolog.Logger,
) {
	c.instancesMutex.Lock()
	clear(c.deployedInstances)
	maps.Copy(c.deployedInstances, discovered)
	instancesCopy := make(map[string]*caddy.Instance, len(c.deployedInstances))
	maps.Copy(instancesCopy, c.deployedInstances)
	c.instancesMutex.Unlock()
	if saveErr := c.stateStore.SaveState(ctx, instancesCopy); saveErr != nil {
		logger.Error().Err(saveErr).Msg("Failed to persist state")
	}
}

func (c *Controller) initializeConfiguration(ctx context.Context, logger zerolog.Logger) {
	if c.config.ExternalEnable {
		c.initializeExternalConfiguration(ctx, logger)
		return
	}
	if c.deployedInstanceCount() == 0 {
		return
	}
	logger.Info().Msg("Pushing initial configuration to discovered instances")
	configData, err := c.loadConfigData(ctx)
	if err != nil {
		c.logConfigLoadError(err, logger, "initial config push")
		return
	}
	c.configHandler.Handle(configData)
}

func (c *Controller) initializeExternalConfiguration(
	ctx context.Context,
	logger zerolog.Logger,
) {
	if c.aggregator == nil {
		logger.Error().
			Msg("External aggregation enabled but aggregator is nil, skipping initial config setup")
		return
	}
	c.loadExternalBatch(ctx, c.aggregator, logger)
	configData, err := c.loadConfigData(ctx)
	if err != nil {
		c.logConfigLoadError(err, logger, "initial config push")
	} else {
		c.aggregator.UpdateBase(configData)
	}
	c.aggregator.MarkInitialized()
	if c.deployedInstanceCount() > 0 {
		logger.Info().Msg("Initial configuration pushed to discovered instances")
		return
	}
	logger.Info().Msg("Aggregator initialized (no instances to push to yet)")
}

func (c *Controller) loadExternalBatch(
	ctx context.Context,
	agg *aggregator.NamespaceAggregator,
	logger zerolog.Logger,
) {
	if c.externalWatcher == nil {
		return
	}
	externals, err := c.externalWatcher.InitialListBatch(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to batch load external ConfigMaps")
		return
	}
	if len(externals) > 0 {
		agg.SetExternalBatch(externals)
	}
}

func (c *Controller) deployedInstanceCount() int {
	c.instancesMutex.RLock()
	defer c.instancesMutex.RUnlock()
	return len(c.deployedInstances)
}

func (c *Controller) loadConfigData(ctx context.Context) (string, error) {
	configMap, err := c.clientset.CoreV1().
		ConfigMaps(c.config.ConfigMapNamespace).
		Get(ctx, c.config.ConfigMapName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	configData, exists := configMap.Data[constants.CaddyfileKey]
	if !exists {
		return "", errConfigMapMissingCaddyfile
	}
	return configData, nil
}

func (c *Controller) logConfigLoadError(
	err error,
	logger zerolog.Logger,
	contextName string,
) {
	if errors.Is(err, errConfigMapMissingCaddyfile) {
		logger.Warn().
			Msgf("ConfigMap missing Caddyfile key, skipping %s", contextName)
		return
	}
	logger.Error().
		Err(err).
		Msgf("Failed to get ConfigMap for %s", contextName)
}

func (c *Controller) reconcileDiscoveredDeployments(
	ctx context.Context,
	discovered map[string]*caddy.Instance,
) {
	logger := log.With().Str("component", "reconcile").Logger()
	for nodeName, adopted := range discovered {
		reconciled, err := caddy.DeployCaddy(
			ctx,
			c.deployOpts,
			nodeName,
			c.config.ExternalEndpoints[nodeName],
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
		logger.Debug().
			Str("node", nodeName).
			Msg("Adopted deployment already matches desired spec")
	}
}

func (c *Controller) runPeriodicConfigReconciliation(ctx context.Context) {
	ticker := time.NewTicker(configReconciliationInterval)
	defer ticker.Stop()
	logger := log.With().Str("component", "config_reconciliation").Logger()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runPeriodicConfigReconciliationTick(ctx, logger)
		}
	}
}

func (c *Controller) runPeriodicConfigReconciliationTick(
	ctx context.Context,
	logger zerolog.Logger,
) {
	logger.Info().Msg("Performing periodic configuration reconciliation")
	configData, err := c.loadConfigData(ctx)
	if err != nil {
		c.logConfigLoadError(err, logger, "reconciliation")
		return
	}
	if !c.applyPeriodicConfigData(configData, logger) {
		return
	}
	logger.Info().Msg("Periodic configuration reconciliation completed")
}

func (c *Controller) applyPeriodicConfigData(
	configData string,
	logger zerolog.Logger,
) bool {
	if !c.config.ExternalEnable {
		c.configHandler.Handle(configData)
		return true
	}
	if c.aggregator == nil {
		logger.Error().
			Msg("External aggregation enabled but aggregator is nil, skipping reconciliation to prevent configuration drift")
		return false
	}
	c.aggregator.UpdateBase(configData)
	return true
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
			if err := c.snapshotAndPersistState(ctx); err != nil {
				logger.Error().
					Err(err).
					Msg("Failed to persist state during periodic save")
			} else {
				logger.Debug().Msg("Periodic state persistence completed")
			}
		}
	}
}

func (c *Controller) persistStateOnShutdown() {
	logger := log.With().Str("component", "state_persistence").Logger()
	logger.Info().Msg("Persisting state before shutdown")
	if err := c.snapshotAndPersistState(context.Background()); err != nil {
		logger.Error().Err(err).Msg("Failed to persist state during shutdown")
	} else {
		logger.Info().Msg("State persisted successfully before shutdown")
	}
}

func (c *Controller) snapshotAndPersistState(ctx context.Context) error {
	c.instancesMutex.RLock()
	if len(c.deployedInstances) == 0 {
		c.instancesMutex.RUnlock()
		return nil
	}
	instancesCopy := make(map[string]*caddy.Instance, len(c.deployedInstances))
	maps.Copy(instancesCopy, c.deployedInstances)
	c.instancesMutex.RUnlock()
	data, err := json.Marshal(instancesCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal state for hash: %w", err)
	}
	hash := sha256.Sum256(data)
	if hash == c.lastPersistedStateHash {
		return nil
	}
	if saveErr := c.stateStore.SaveState(ctx, instancesCopy); saveErr != nil {
		return saveErr
	}
	c.lastPersistedStateHash = hash
	return nil
}
