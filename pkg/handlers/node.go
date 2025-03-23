package handlers

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"k8s.io/client-go/kubernetes"

	"gl.vprw.ru/vapronva/ckic/pkg/caddy"
	"gl.vprw.ru/vapronva/ckic/pkg/watcher"
)

type deploymentJob struct {
	nodeName string
	resultCh chan *deploymentResult
}

type deploymentResult struct {
	nodeName string
	instance *caddy.Instance
	err      error
}

type NodeHandler struct {
	Clientset          *kubernetes.Clientset
	Namespace          string
	CaddyImage         string
	EnableLoadBalancer bool
	DeployedInstances  map[string]*caddy.Instance
	Mu                 *sync.RWMutex
	jobCh              chan deploymentJob
	nodeChangeNotifier func()
	EnvSecretName      string
	EnvSecretKeys      []string
	DataVolumePVC      string
	ConfigVolumePVC    string
}

func NewNodeHandler(
	clientset *kubernetes.Clientset,
	namespace,
	caddyImage string,
	enableLoadBalancer bool,
	instances map[string]*caddy.Instance,
	mu *sync.RWMutex,
	notifier func(),
	envSecretName string,
	envSecretKeys []string,
	dataVolumePVC string,
	configVolumePVC string,
) *NodeHandler {
	return &NodeHandler{
		Clientset:          clientset,
		Namespace:          namespace,
		CaddyImage:         caddyImage,
		EnableLoadBalancer: enableLoadBalancer,
		DeployedInstances:  instances,
		Mu:                 mu,
		nodeChangeNotifier: notifier,
		EnvSecretName:      envSecretName,
		EnvSecretKeys:      envSecretKeys,
		DataVolumePVC:      dataVolumePVC,
		ConfigVolumePVC:    configVolumePVC,
	}
}

func (h *NodeHandler) SetNodeChangeNotifier(notifier func()) {
	h.nodeChangeNotifier = notifier
}

func (h *NodeHandler) StartWorkerPool(ctx context.Context, workerCount int) {
	h.jobCh = make(chan deploymentJob, 100)
	for range workerCount {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-h.jobCh:
					if !ok {
						return
					}
					deployCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
					instance, err := caddy.DeployCaddy(
						deployCtx,
						h.Clientset,
						job.nodeName,
						h.Namespace,
						h.CaddyImage,
						h.EnableLoadBalancer,
						h.EnvSecretName,
						h.EnvSecretKeys,
						h.DataVolumePVC,
						h.ConfigVolumePVC,
					)
					cancel()
					job.resultCh <- &deploymentResult{
						nodeName: job.nodeName,
						instance: instance,
						err:      err,
					}
				}
			}
		}()
	}
}

func (h *NodeHandler) Handle(event watcher.NodeEvent) {
	nodeName := event.NodeName
	logger := log.With().Str("node", nodeName).Logger()
	switch event.Type {
	case watcher.NodeAdded:
		logger.Info().Msg("Detected new node, deploying Caddy")
		resultCh := make(chan *deploymentResult, 1)
		h.jobCh <- deploymentJob{
			nodeName: nodeName,
			resultCh: resultCh,
		}
		go func() {
			result := <-resultCh
			if result.err != nil {
				logger.Error().Err(result.err).Msg("Failed to deploy Caddy instance")
				return
			}
			h.Mu.Lock()
			h.DeployedInstances[nodeName] = result.instance
			h.Mu.Unlock()
			logger.Info().Msg("Successfully deployed Caddy instance")
			if h.nodeChangeNotifier != nil {
				h.nodeChangeNotifier()
			}
		}()
	case watcher.NodeRemoved:
		h.Mu.Lock()
		defer h.Mu.Unlock()
		if instance, exists := h.DeployedInstances[nodeName]; exists {
			logger.Info().Msg("Node removed, cleaning up Caddy instance")
			if err := instance.Delete(); err != nil {
				logger.Error().Err(err).Msg("Failed to delete Caddy instance")
			} else {
				delete(h.DeployedInstances, nodeName)
				logger.Info().Msg("Successfully removed Caddy instance")
			}
			if h.nodeChangeNotifier != nil {
				h.nodeChangeNotifier()
			}
		}
	}
}
