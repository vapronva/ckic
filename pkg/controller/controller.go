package controller

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"gl.vprw.ru/vapronva/ckic/pkg/caddy"
	"gl.vprw.ru/vapronva/ckic/pkg/utils"
)

type ControllerConfig struct {
	Kubeconfig          string
	NodeLabel           string
	ConfigMapName       string
	ConfigMapNamespace  string
	CommunicationMethod string // "clusterip" or "direct"
	CaddyImage          string
	EnableNodePort      bool
}

type Controller struct {
	clientset           *kubernetes.Clientset
	config              ControllerConfig
	nodeWatcher         *NodeWatcher
	configWatcher       *ConfigWatcher
	deployedInstances   map[string]*caddy.Instance
	deployedInstancesMu sync.RWMutex
}

func NewController(ctx context.Context, config ControllerConfig) (*Controller, error) {
	clientset, err := utils.GetKubernetesClient(config.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubernetes client: %w", err)
	}
	controller := &Controller{
		clientset:         clientset,
		config:            config,
		deployedInstances: make(map[string]*caddy.Instance),
	}
	nodeWatcher, err := NewNodeWatcher(ctx, clientset, config.NodeLabel, controller.handleNodeChange)
	if err != nil {
		return nil, fmt.Errorf("failed to create node watcher: %w", err)
	}
	controller.nodeWatcher = nodeWatcher
	configWatcher, err := NewConfigWatcher(ctx, clientset, config.ConfigMapNamespace, config.ConfigMapName, controller.handleConfigChange)
	if err != nil {
		return nil, fmt.Errorf("failed to create config watcher: %w", err)
	}
	controller.configWatcher = configWatcher
	return controller, nil
}

func (c *Controller) Run(ctx context.Context) error {
	if err := c.ReconcileState(ctx); err != nil {
		log.Error().Err(err).Msg("Reconciliation failed")
		return err
	}
	go c.nodeWatcher.Start(ctx)
	go c.configWatcher.Start(ctx)
	<-ctx.Done()
	log.Info().Msg("Controller shutting down")
	return nil
}

func (c *Controller) ReconcileState(ctx context.Context) error {
	logger := log.With().Str("component", "reconcile").Logger()
	logger.Info().Msg("Starting reconciliation process")
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
			podList, err := c.clientset.CoreV1().Pods(c.config.ConfigMapNamespace).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("app=caddy,instance=%s", nodeName),
			})
			if err != nil || len(podList.Items) == 0 {
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
			c.deployedInstancesMu.Lock()
			c.deployedInstances[nodeName] = instance
			c.deployedInstancesMu.Unlock()
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
			if err := instance.Delete(c.clientset); err != nil {
				logger.Error().Err(err).Msgf("Failed to delete orphaned deployment on node %s", nodeName)
			} else {
				logger.Info().Msgf("Deleted orphaned deployment on node %s", nodeName)
			}
		}
	}
	logger.Info().Msg("Reconciliation process completed")
	return nil
}

func (c *Controller) handleNodeChange(nodeEvent NodeEvent) {
	nodeName := nodeEvent.NodeName
	logger := log.With().Str("node", nodeName).Logger()
	c.deployedInstancesMu.Lock()
	defer c.deployedInstancesMu.Unlock()
	switch nodeEvent.Type {
	case NodeAdded:
		logger.Info().Msg("Detected new node with CKIC label, deploying Caddy")
		instance, err := caddy.DeployCaddy(c.clientset, nodeName, c.config.ConfigMapNamespace, c.config.CaddyImage, c.config.EnableNodePort)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to deploy Caddy instance")
			return
		}
		c.deployedInstances[nodeName] = instance
		logger.Info().Msg("Successfully deployed Caddy instance")
	case NodeRemoved:
		if instance, exists := c.deployedInstances[nodeName]; exists {
			logger.Info().Msg("Node no longer has CKIC label, removing Caddy instance")
			if err := instance.Delete(c.clientset); err != nil {
				logger.Error().Err(err).Msg("Failed to clean up Caddy instance")
			} else {
				delete(c.deployedInstances, nodeName)
				logger.Info().Msg("Successfully removed Caddy instance")
			}
		}
	}
}

func (c *Controller) handleConfigChange(configData string) {
	logger := log.With().Str("event", "config_change").Logger()
	logger.Info().Msg("Detected configuration change, updating Caddy instances")
	c.deployedInstancesMu.RLock()
	var instances []struct {
		nodeName string
		instance *caddy.Instance
	}
	for nodeName, inst := range c.deployedInstances {
		instances = append(instances, struct {
			nodeName string
			instance *caddy.Instance
		}{nodeName, inst})
	}
	c.deployedInstancesMu.RUnlock()
	for _, entry := range instances {
		instanceLogger := logger.With().Str("node", entry.nodeName).Logger()
		instanceLogger.Debug().Msg("Updating Caddy configuration")
		err := entry.instance.UpdateConfig(configData, c.config.CommunicationMethod)
		if err != nil {
			instanceLogger.Error().Err(err).Msg("Failed to update Caddy configuration")
			if entry.instance.FailureCount >= 5 {
				instanceLogger.Warn().Msg("Too many configuration update failures, redeploying instance")
				if err := entry.instance.Delete(c.clientset); err != nil {
					instanceLogger.Error().Err(err).Msg("Failed to delete failed Caddy instance")
					continue
				}
				newInstance, err := caddy.DeployCaddy(c.clientset, entry.nodeName, c.config.ConfigMapNamespace, c.config.CaddyImage, c.config.EnableNodePort)
				if err != nil {
					instanceLogger.Error().Err(err).Msg("Failed to redeploy Caddy instance")
					continue
				}
				c.deployedInstancesMu.Lock()
				c.deployedInstances[entry.nodeName] = newInstance
				c.deployedInstancesMu.Unlock()
				instanceLogger.Info().Msg("Successfully redeployed Caddy instance")
			}
		} else {
			instanceLogger.Info().Msg("Successfully updated Caddy configuration")
			entry.instance.FailureCount = 0
		}
	}
}
