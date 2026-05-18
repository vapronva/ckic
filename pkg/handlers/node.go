package handlers

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

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
	deployOpts         caddy.DeployOptions
	deployedInstances  map[string]*caddy.Instance
	mu                 *sync.RWMutex
	externalEndpoints  utils.ExternalEndpointsMap
	jobCh              chan deploymentJob
	nodeChangeNotifier func()
	inProgressNodes    map[string]struct{}
	inProgressNodesMu  sync.Mutex
	removedNodes       map[string]struct{}
	lifetimeCtx        context.Context
	deployFn           func(
		ctx context.Context,
		opts caddy.DeployOptions,
		nodeName string,
		externalIPs []string,
	) (*caddy.Instance, error)
}

const (
	deploymentQueueSize = 100
	deployJobTimeout    = 3 * time.Minute
)

func NewNodeHandler(
	deployOpts caddy.DeployOptions,
	instances map[string]*caddy.Instance,
	mu *sync.RWMutex,
	externalEndpoints utils.ExternalEndpointsMap,
	notifier func(),
) *NodeHandler {
	return &NodeHandler{
		deployOpts:         deployOpts,
		deployedInstances:  instances,
		mu:                 mu,
		externalEndpoints:  externalEndpoints,
		nodeChangeNotifier: notifier,
		inProgressNodes:    make(map[string]struct{}),
		removedNodes:       make(map[string]struct{}),
		deployFn:           caddy.DeployCaddy,
	}
}

func (h *NodeHandler) SetNodeChangeNotifier(notifier func()) {
	h.nodeChangeNotifier = notifier
}

func (h *NodeHandler) StartWorkerPool(ctx context.Context, workerCount int) {
	h.lifetimeCtx = ctx
	h.jobCh = make(chan deploymentJob, deploymentQueueSize)
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
					deployFn := h.deployFn
					if deployFn == nil {
						deployFn = caddy.DeployCaddy
					}
					deployCtx, cancel := context.WithTimeout(ctx, deployJobTimeout)
					instance, err := deployFn(
						deployCtx,
						h.deployOpts,
						job.nodeName,
						job.externalIPs,
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
	switch event.Type {
	case watcher.NodeAdded:
		h.handleNodeAdded(event.NodeName)
	case watcher.NodeRemoved:
		h.handleNodeRemoved(event.NodeName)
	}
}

func (h *NodeHandler) handleNodeAdded(nodeName string) {
	logger := log.With().Str("node", nodeName).Logger()
	h.mu.RLock()
	_, alreadyDeployed := h.deployedInstances[nodeName]
	h.mu.RUnlock()
	h.inProgressNodesMu.Lock()
	if _, inProgress := h.inProgressNodes[nodeName]; inProgress {
		delete(h.removedNodes, nodeName)
		h.inProgressNodesMu.Unlock()
		logger.Debug().
			Msg("Deployment already in progress for node, cleared removal flag")
		return
	}
	h.inProgressNodes[nodeName] = struct{}{}
	h.inProgressNodesMu.Unlock()
	if alreadyDeployed {
		logger.Debug().
			Msg("Node already has a Caddy deployment, reconciling desired state")
	} else {
		logger.Info().Msg("Detected new node, deploying Caddy")
	}
	resultCh := make(chan *deploymentResult, 1)
	externalIPs, exists := h.externalEndpoints[nodeName]
	if exists {
		logger.Info().
			Strs("externalIPs", externalIPs).
			Msg("Found external endpoints for node")
	}
	h.jobCh <- deploymentJob{
		nodeName:    nodeName,
		resultCh:    resultCh,
		externalIPs: externalIPs,
	}
	go func() {
		h.handleNodeAddedResult(nodeName, alreadyDeployed, <-resultCh)
	}()
}

func (h *NodeHandler) handleNodeRemoved(nodeName string) {
	logger := log.With().Str("node", nodeName).Logger()
	var (
		shouldNotify bool
		instance     *caddy.Instance
		exists       bool
	)
	h.inProgressNodesMu.Lock()
	if _, inProgress := h.inProgressNodes[nodeName]; inProgress {
		h.removedNodes[nodeName] = struct{}{}
		h.inProgressNodesMu.Unlock()
		logger.Info().
			Msg("Node removed while deployment in progress, marking for cleanup")
		return
	}
	h.mu.Lock()
	instance, exists = h.deployedInstances[nodeName]
	if exists {
		delete(h.deployedInstances, nodeName)
		shouldNotify = h.nodeChangeNotifier != nil
	}
	h.mu.Unlock()
	h.inProgressNodesMu.Unlock()
	if !exists {
		return
	}
	logger.Info().Msg("Node removed, cleaning up Caddy instance")
	if err := instance.Delete(h.ctx()); err != nil {
		logger.Error().Err(err).Msg("Failed to delete Caddy instance")
		restoreInstanceIfMissing(h.mu, h.deployedInstances, nodeName, instance)
		return
	}
	logger.Info().Msg("Successfully removed Caddy instance")
	if shouldNotify {
		h.nodeChangeNotifier()
	}
}

func (h *NodeHandler) MarkInProgress(nodeName string) bool {
	h.inProgressNodesMu.Lock()
	defer h.inProgressNodesMu.Unlock()
	if _, exists := h.inProgressNodes[nodeName]; exists {
		return false
	}
	h.inProgressNodes[nodeName] = struct{}{}
	return true
}

func (h *NodeHandler) UnmarkInProgress(
	nodeName string,
) (bool, *caddy.Instance) {
	h.inProgressNodesMu.Lock()
	defer h.inProgressNodesMu.Unlock()
	delete(h.inProgressNodes, nodeName)
	if _, wasRemoved := h.removedNodes[nodeName]; !wasRemoved {
		return false, nil
	}
	delete(h.removedNodes, nodeName)
	h.mu.Lock()
	defer h.mu.Unlock()
	instance := h.deployedInstances[nodeName]
	delete(h.deployedInstances, nodeName)
	return true, instance
}

func (h *NodeHandler) CleanupRemovedNode(
	nodeName string,
	instance *caddy.Instance,
) {
	logger := log.With().Str("node", nodeName).Logger()
	if instance == nil {
		logger.Debug().
			Msg("No instance to clean up after redeploy/removal race")
		return
	}
	if err := instance.Delete(h.ctx()); err != nil {
		logger.Error().
			Err(err).
			Msg("Failed to delete orphaned Caddy instance after redeploy/removal race")
		return
	}
	logger.Info().
		Msg("Successfully cleaned up orphaned Caddy instance after redeploy/removal race")
	h.notifyNodeChange()
}

func (h *NodeHandler) handleNodeAddedResult(
	nodeName string,
	alreadyDeployed bool,
	result *deploymentResult,
) {
	logger := log.With().Str("node", nodeName).Logger()
	defer func() {
		h.inProgressNodesMu.Lock()
		delete(h.inProgressNodes, nodeName)
		h.inProgressNodesMu.Unlock()
	}()
	h.inProgressNodesMu.Lock()
	h.mu.Lock()
	previousInstance := h.deployedInstances[nodeName]
	_, wasRemoved := h.removedNodes[nodeName]
	if wasRemoved {
		delete(h.removedNodes, nodeName)
		delete(h.deployedInstances, nodeName)
	}
	h.inProgressNodesMu.Unlock()
	if result.err != nil {
		h.mu.Unlock()
		h.handleNodeAddDeployError(nodeName, previousInstance, wasRemoved, result.err)
		return
	}
	if wasRemoved {
		h.mu.Unlock()
		h.handleRemovedNodeAfterDeployResult(nodeName, previousInstance, result.instance)
		return
	}
	h.deployedInstances[nodeName] = result.instance
	instanceChanged := !instancesEquivalent(previousInstance, result.instance)
	h.mu.Unlock()
	switch {
	case !alreadyDeployed:
		logger.Info().Msg("Successfully deployed Caddy instance")
	case instanceChanged:
		logger.Info().Msg("Reconciled existing Caddy deployment")
	default:
		logger.Debug().Msg("Existing Caddy deployment is already up-to-date")
	}
	if !alreadyDeployed || instanceChanged {
		h.notifyNodeChange()
	}
}

func (h *NodeHandler) handleNodeAddDeployError(
	nodeName string,
	previousInstance *caddy.Instance,
	wasRemoved bool,
	deployErr error,
) {
	logger := log.With().Str("node", nodeName).Logger()
	logger.Error().Err(deployErr).Msg("Failed to deploy Caddy instance")
	if !wasRemoved || previousInstance == nil {
		return
	}
	logger.Info().
		Msg("Node was removed while deployment failed, cleaning up previous instance")
	if err := previousInstance.Delete(h.ctx()); err != nil {
		logger.Error().
			Err(err).
			Msg("Failed to delete previous Caddy instance after removal")
		restoreInstanceIfMissing(h.mu, h.deployedInstances, nodeName, previousInstance)
		return
	}
	h.notifyNodeChange()
}

func (h *NodeHandler) handleRemovedNodeAfterDeployResult(
	nodeName string,
	previousInstance, deployedInstance *caddy.Instance,
) {
	logger := log.With().Str("node", nodeName).Logger()
	logger.Info().Msg("Node was removed during deployment, cleaning up")
	cleanupInstance := deployedInstance
	if cleanupInstance == nil {
		cleanupInstance = previousInstance
	}
	if cleanupInstance == nil {
		return
	}
	if err := cleanupInstance.Delete(h.ctx()); err != nil {
		logger.Error().Err(err).Msg("Failed to delete Caddy instance after removal")
		restoreInstanceIfMissing(h.mu, h.deployedInstances, nodeName, previousInstance)
		return
	}
	if previousInstance != nil {
		h.notifyNodeChange()
	}
}

func restoreInstanceIfMissing(
	mu *sync.RWMutex,
	instances map[string]*caddy.Instance,
	nodeName string,
	instance *caddy.Instance,
) {
	if instance == nil {
		return
	}
	mu.Lock()
	if _, exists := instances[nodeName]; !exists {
		instances[nodeName] = instance
	}
	mu.Unlock()
}

func (h *NodeHandler) notifyNodeChange() {
	if h.nodeChangeNotifier != nil {
		h.nodeChangeNotifier()
	}
}

func (h *NodeHandler) ctx() context.Context {
	if h.lifetimeCtx == nil {
		return context.Background()
	}
	return h.lifetimeCtx
}

func instancesEquivalent(a, b *caddy.Instance) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.StateKey() == b.StateKey()
}
