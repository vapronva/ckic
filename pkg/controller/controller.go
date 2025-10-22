package controller

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"gl.vprw.ru/vapronva/ckic/pkg/aggregator"
	"gl.vprw.ru/vapronva/ckic/pkg/caddy"
	"gl.vprw.ru/vapronva/ckic/pkg/constants"
	"gl.vprw.ru/vapronva/ckic/pkg/handlers"
	"gl.vprw.ru/vapronva/ckic/pkg/state"
	"gl.vprw.ru/vapronva/ckic/pkg/utils"
	"gl.vprw.ru/vapronva/ckic/pkg/watcher"
)

type ControllerConfig struct {
	Kubeconfig                   string
	NodeLabel                    string
	ConfigMapName                string
	ConfigMapNamespace           string
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
	clientset         *kubernetes.Clientset
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

func NewController(clientset *kubernetes.Clientset, config ControllerConfig) (*Controller, error) {
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
		config.ExternalEndpoints,
		config.UseHostNetwork,
		config.CaddyAdminOriginKey,
		config.HTTPHostPort,
		config.HTTPSHostPort,
	)
	var configWatcher *watcher.ConfigWatcher
	var externalWatcher *watcher.ExternalConfigWatcher
	var agg *aggregator.NamespaceAggregator
	if config.ExternalEnable {
		agg = aggregator.NewNamespaceAggregator(
			clientset,
			config.ConfigMapNamespace,
			config.ExternalPublishAggregated,
			config.ExternalAggregatedConfigName,
			configHandler.Handle,
			nodeAvailabilityCheck,
		)
		configWatcher = watcher.NewConfigWatcher(
			clientset,
			config.ConfigMapNamespace,
			config.ConfigMapName,
			agg.UpdateBase,
			nodeAvailabilityCheck,
		)
		externalWatcher = watcher.NewExternalConfigWatcher(
			clientset,
			config.ConfigMapNamespace,
			config.ConfigMapName,
			config.ExternalLabel,
			config.ExternalNsMode,
			config.ExternalAllowNamespaces,
			config.ExternalDenyNamespaces,
			agg.SetExternal,
			agg.RemoveExternal,
		)
	} else {
		configWatcher = watcher.NewConfigWatcher(
			clientset,
			config.ConfigMapNamespace,
			config.ConfigMapName,
			configHandler.Handle,
			nodeAvailabilityCheck,
		)
	}
	coordinator := NewWatcherCoordinator(nodeWatcher, configWatcher, deployedInstances)
	nodeHandler.SetNodeChangeNotifier(coordinator.NotifyNodeChange)
	stateStore := state.NewConfigMapStateStore(clientset, config.ConfigMapNamespace, constants.StateConfigMapName)
	ctrl := &Controller{
		clientset:         clientset,
		config:            config,
		nodeWatcher:       nodeWatcher,
		configWatcher:     configWatcher,
		externalWatcher:   externalWatcher,
		nodeHandler:       nodeHandler,
		configHandler:     configHandler,
		deployedInstances: deployedInstances,
		instancesMutex:    mutex,
		stateStore:        stateStore,
		coordinator:       coordinator,
	}
	if agg != nil {
		ctrl.aggregator = agg
	}
	return ctrl, nil
}

func (c *Controller) Run(ctx context.Context) error {
	if err := c.ReconcileState(ctx); err != nil {
		log.Error().Err(err).Msg("Reconciliation failed")
		return err
	}
	c.nodeHandler.StartWorkerPool(ctx, 4)
	go c.nodeWatcher.Start(ctx)
	go c.configWatcher.Start(ctx)
	if c.config.ExternalEnable && c.externalWatcher != nil {
		log.Info().Msg("Starting external ConfigMap watcher")
		go c.externalWatcher.Start(ctx)
	}
	go c.runPeriodicConfigReconciliation(ctx)
	<-ctx.Done()
	log.Info().Msg("Controller shutting down")
	return nil
}

func (c *Controller) ReconcileState(ctx context.Context) error {
	logger := log.With().Str("component", "reconcile").Logger()
	logger.Info().Msg("Starting reconciliation process")
	discovered := make(map[string]*caddy.Instance)
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
		if dep.Status.ReadyReplicas > 0 {
			podList, errPL := c.clientset.CoreV1().Pods(c.config.ConfigMapNamespace).List(ctx, metav1.ListOptions{
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
				FailureCount:   0,
				KubeClient:     c.clientset,
			}
			discovered[nodeName] = instance
			logger.Info().Msgf("Adopted existing healthy deployment on node %s", nodeName)
		} else {
			logger.Warn().Msgf("Deployment on node %s is not healthy, deleting orphaned deployment", nodeName)
			instance := &caddy.Instance{
				NodeName:       nodeName,
				Namespace:      c.config.ConfigMapNamespace,
				DeploymentName: dep.Name,
				ServiceName:    dep.Name,
				KubeClient:     c.clientset,
			}
			if errID := instance.Delete(); errID != nil {
				logger.Error().Err(errID).Msgf("Failed to delete orphaned deployment on node %s", nodeName)
			} else {
				logger.Info().Msgf("Deleted orphaned deployment on node %s", nodeName)
			}
		}
	}
	savedState, err := c.stateStore.LoadState()
	if err != nil {
		logger.Warn().Err(err).Msg("Could not load saved state, proceeding with discovered state")
	}
	if c.config.PreferSavedState && len(savedState) > 0 {
		logger.Info().Msg("PreferSavedState is enabled. Merging saved state with discovered state, preferring saved state")
		for node := range discovered {
			if savedInst, exists := savedState[node]; exists {
				discovered[node] = savedInst
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
	c.instancesMutex.Unlock()
	if err := c.stateStore.SaveState(c.deployedInstances); err != nil {
		logger.Error().Err(err).Msg("Failed to persist state")
	}
	if len(c.deployedInstances) > 0 {
		logger.Info().Msg("Pushing initial configuration to discovered instances")
		configMap, err := c.clientset.CoreV1().ConfigMaps(c.config.ConfigMapNamespace).Get(
			ctx, c.config.ConfigMapName, metav1.GetOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to get ConfigMap for initial config push")
		} else if configData, exists := configMap.Data["Caddyfile"]; exists {
			if c.config.ExternalEnable {
				if c.aggregator == nil {
					logger.Error().Msg("External aggregation enabled but aggregator is nil, skipping initial config push")
				} else if agg, ok := c.aggregator.(*aggregator.NamespaceAggregator); ok {
					agg.UpdateBase(configData)
				} else {
					logger.Error().Msg("Failed to assert aggregator type, skipping initial config push")
				}
			} else {
				c.configHandler.Handle(configData)
			}
		} else {
			logger.Warn().Msg("ConfigMap missing Caddyfile key, skipping initial config push")
		}
	}
	logger.Info().Msg("Reconciliation process completed")
	return nil
}

func (c *Controller) runPeriodicConfigReconciliation(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
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
					logger.Error().Msg("External aggregation enabled but aggregator is nil, skipping reconciliation to prevent configuration drift")
					continue
				}
				if agg, ok := c.aggregator.(*aggregator.NamespaceAggregator); ok {
					agg.UpdateBase(configData)
				} else {
					logger.Error().Msg("Failed to assert aggregator type, skipping reconciliation to prevent configuration drift")
					continue
				}
			} else {
				c.configHandler.Handle(configData)
			}
			logger.Info().Msg("Periodic configuration reconciliation completed")
		}
	}
}
