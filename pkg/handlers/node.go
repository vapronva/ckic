package handlers

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/caddy"
	"git.horse/vapronva/ckic/pkg/utils"
	"git.horse/vapronva/ckic/pkg/watcher"
)

type deploymentJob struct {
	nodeName    string
	resultCh    chan *deploymentResult
	externalIPs []string
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
	ConfigMapName      string
	ExternalEndpoints  utils.ExternalEndpointsMap
	UseHostNetwork     bool
	HTTPHostPort       int
	HTTPSHostPort      int
	inProgressNodes    map[string]struct{}
	inProgressNodesMu  sync.Mutex
	removedNodes       map[string]struct{}
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
	configMapName string,
	externalEndpoints utils.ExternalEndpointsMap,
	useHostNetwork bool,
	httpHostPort int,
	httpsHostPort int,
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
		ConfigMapName:      configMapName,
		ExternalEndpoints:  externalEndpoints,
		UseHostNetwork:     useHostNetwork,
		HTTPHostPort:       httpHostPort,
		HTTPSHostPort:      httpsHostPort,
		inProgressNodes:    make(map[string]struct{}),
		removedNodes:       make(map[string]struct{}),
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
						job.externalIPs,
						h.EnvSecretName,
						h.EnvSecretKeys,
						h.DataVolumePVC,
						h.ConfigVolumePVC,
						h.ConfigMapName,
						h.UseHostNetwork,
						h.HTTPHostPort,
						h.HTTPSHostPort,
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
		h.Mu.RLock()
		_, alreadyDeployed := h.DeployedInstances[nodeName]
		h.Mu.RUnlock()
		if alreadyDeployed {
			logger.Debug().Msg("Node already has a Caddy deployment, skipping")
			return
		}
		h.inProgressNodesMu.Lock()
		if _, inProgress := h.inProgressNodes[nodeName]; inProgress {
			delete(h.removedNodes, nodeName)
			h.inProgressNodesMu.Unlock()
			logger.Debug().Msg("Deployment already in progress for node, cleared removal flag")
			return
		}
		h.inProgressNodes[nodeName] = struct{}{}
		h.inProgressNodesMu.Unlock()
		logger.Info().Msg("Detected new node, deploying Caddy")
		resultCh := make(chan *deploymentResult, 1)
		externalIPs, exists := h.ExternalEndpoints[nodeName]
		if exists {
			logger.Info().Strs("externalIPs", externalIPs).Msg("Found external endpoints for node")
		}
		h.jobCh <- deploymentJob{
			nodeName:    nodeName,
			resultCh:    resultCh,
			externalIPs: externalIPs,
		}
		go func() {
			result := <-resultCh
			if result.err != nil {
				h.inProgressNodesMu.Lock()
				delete(h.inProgressNodes, nodeName)
				h.inProgressNodesMu.Unlock()
				logger.Error().Err(result.err).Msg("Failed to deploy Caddy instance")
				return
			}
			h.inProgressNodesMu.Lock()
			h.Mu.Lock()
			delete(h.inProgressNodes, nodeName)
			_, wasRemoved := h.removedNodes[nodeName]
			if wasRemoved {
				delete(h.removedNodes, nodeName)
			}
			h.inProgressNodesMu.Unlock()
			if wasRemoved {
				h.Mu.Unlock()
				logger.Info().Msg("Node was removed during deployment, cleaning up")
				if err := result.instance.Delete(); err != nil {
					logger.Error().Err(err).Msg("Failed to delete Caddy instance after removal")
				}
				return
			}
			h.DeployedInstances[nodeName] = result.instance
			h.Mu.Unlock()
			logger.Info().Msg("Successfully deployed Caddy instance")
			if h.nodeChangeNotifier != nil {
				h.nodeChangeNotifier()
			}
		}()
	case watcher.NodeRemoved:
		var shouldNotify bool
		h.inProgressNodesMu.Lock()
		if _, inProgress := h.inProgressNodes[nodeName]; inProgress {
			h.removedNodes[nodeName] = struct{}{}
			h.inProgressNodesMu.Unlock()
			logger.Info().Msg("Node removed while deployment in progress, marking for cleanup")
			return
		}
		h.inProgressNodesMu.Unlock()
		h.Mu.Lock()
		if instance, exists := h.DeployedInstances[nodeName]; exists {
			logger.Info().Msg("Node removed, cleaning up Caddy instance")
			if err := instance.Delete(); err != nil {
				logger.Error().Err(err).Msg("Failed to delete Caddy instance")
			} else {
				delete(h.DeployedInstances, nodeName)
				logger.Info().Msg("Successfully removed Caddy instance")
			}
			shouldNotify = h.nodeChangeNotifier != nil
		}
		h.Mu.Unlock()
		if shouldNotify {
			h.nodeChangeNotifier()
		}
	}
}
