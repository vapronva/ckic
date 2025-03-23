package controller

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"gl.vprw.ru/vapronva/ckic/pkg/caddy"
	"gl.vprw.ru/vapronva/ckic/pkg/handlers"
	"gl.vprw.ru/vapronva/ckic/pkg/watcher"
)

type ControllerConfig struct {
	Kubeconfig          string
	NodeLabel           string
	ConfigMapName       string
	ConfigMapNamespace  string
	CommunicationMethod caddy.CommunicationMethod
	CaddyImage          string
	EnableNodePort      bool
}

type Controller struct {
	clientset         *kubernetes.Clientset
	config            ControllerConfig
	nodeWatcher       *watcher.NodeWatcher
	configWatcher     *watcher.ConfigWatcher
	nodeHandler       *handlers.NodeHandler
	configHandler     *handlers.ConfigHandler
	deployedInstances map[string]*caddy.Instance
}

func NewController(clientset *kubernetes.Clientset, config ControllerConfig) (*Controller, error) {
	deployedInstances := make(map[string]*caddy.Instance)
	mutex := &sync.RWMutex{}
	nodeHandler := handlers.NewNodeHandler(clientset, config.ConfigMapNamespace, config.CaddyImage, config.EnableNodePort, deployedInstances, mutex)
	nodeWatcher := watcher.NewNodeWatcher(clientset, config.NodeLabel, nodeHandler.Handle)
	configHandler := handlers.NewConfigHandler(config.CommunicationMethod, clientset, config.ConfigMapNamespace, config.CaddyImage, config.EnableNodePort, deployedInstances, mutex)
	configWatcher := watcher.NewConfigWatcher(clientset, config.ConfigMapNamespace, config.ConfigMapName, configHandler.Handle)
	return &Controller{
		clientset:         clientset,
		config:            config,
		nodeWatcher:       nodeWatcher,
		configWatcher:     configWatcher,
		nodeHandler:       nodeHandler,
		configHandler:     configHandler,
		deployedInstances: deployedInstances,
	}, nil
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
				LabelSelector: "app=caddy,instance=" + nodeName,
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
			c.deployedInstances[nodeName] = instance
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
			if err := instance.Delete(); err != nil {
				logger.Error().Err(err).Msgf("Failed to delete orphaned deployment on node %s", nodeName)
			} else {
				logger.Info().Msgf("Deleted orphaned deployment on node %s", nodeName)
			}
		}
	}
	logger.Info().Msg("Reconciliation process completed")
	return nil
}
