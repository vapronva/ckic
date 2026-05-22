package watcher

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/utils"
)

type NodeEventType int

const (
	NodeAdded NodeEventType = iota
	NodeRemoved
)

const (
	nodeListRetryDelay  = 10 * time.Second
	nodeWatchRetryDelay = 5 * time.Second
)

type NodeEvent struct {
	Type     NodeEventType
	NodeName string
}

type NodeHandler func(NodeEvent)

type NodeWatcher struct {
	clientset           kubernetes.Interface
	labelSelector       string
	parsedSelector      labels.Selector
	nodeHandler         NodeHandler
	mu                  sync.RWMutex
	currentNodes        map[string]struct{}
	lastResourceVersion string
}

func normalizeLegacyNodeSelector(labelSelector string) string {
	const legacyNodeSelectorParts = 2
	selector := strings.TrimSpace(labelSelector)
	if strings.Count(selector, ":") != 1 {
		return selector
	}
	if strings.Contains(selector, "=") ||
		strings.Contains(selector, ",") ||
		strings.Contains(selector, "(") ||
		strings.Contains(selector, ")") ||
		strings.Contains(selector, "!") ||
		strings.Contains(selector, " in ") ||
		strings.Contains(selector, " notin ") {
		return selector
	}
	parts := strings.SplitN(selector, ":", legacyNodeSelectorParts)
	key := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])
	value = strings.Trim(value, "\"' ")
	if key == "" || value == "" {
		return selector
	}
	return key + "=" + value
}

func NormalizeNodeLabelSelector(labelSelector string) (string, error) {
	selector := strings.TrimSpace(labelSelector)
	if selector == "" {
		return "", nil
	}
	selector = normalizeLegacyNodeSelector(selector)
	parsed, err := labels.Parse(selector)
	if err != nil {
		return "", fmt.Errorf("invalid node label selector %q: %w", selector, err)
	}
	return parsed.String(), nil
}

func NewNodeWatcher(
	clientset kubernetes.Interface,
	labelSelector string,
	handler NodeHandler,
) (*NodeWatcher, error) {
	normalizedSelector, err := NormalizeNodeLabelSelector(labelSelector)
	if err != nil {
		return nil, err
	}
	parsed, parseErr := labels.Parse(normalizedSelector)
	if parseErr != nil {
		return nil, fmt.Errorf(
			"failed to parse normalized selector %q: %w",
			normalizedSelector,
			parseErr,
		)
	}
	return &NodeWatcher{
		clientset:      clientset,
		labelSelector:  normalizedSelector,
		parsedSelector: parsed,
		nodeHandler:    handler,
		currentNodes:   make(map[string]struct{}),
	}, nil
}

func (w *NodeWatcher) Start(ctx context.Context) {
	logger := log.With().
		Str("component", "node_watcher").
		Str("label_selector", w.labelSelector).
		Logger()
	logger.Info().Msg("Starting node watcher")
	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Node watcher shutting down")
			return
		default:
		}
		if err := w.syncCurrentNodes(ctx); err != nil {
			logger.Error().Err(err).Msg("Failed to list nodes")
			if !utils.SleepCtx(ctx, nodeListRetryDelay) {
				return
			}
			continue
		}
		watcher, err := w.clientset.CoreV1().Nodes().Watch(ctx, metav1.ListOptions{
			LabelSelector:   w.labelSelector,
			ResourceVersion: w.lastResourceVersion,
		})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create node watcher")
			if !utils.SleepCtx(ctx, nodeListRetryDelay) {
				return
			}
			continue
		}
		if !w.consumeWatchEvents(ctx, watcher, logger) {
			return
		}
		logger.Info().Msg("Node watcher channel closed, restarting")
		if !utils.SleepCtx(ctx, nodeWatchRetryDelay) {
			return
		}
	}
}

func (w *NodeWatcher) syncCurrentNodes(ctx context.Context) error {
	nodes, err := w.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: w.labelSelector,
	})
	if err != nil {
		return err
	}
	w.lastResourceVersion = nodes.ResourceVersion
	newNodes := make(map[string]struct{}, len(nodes.Items))
	for _, node := range nodes.Items {
		newNodes[node.Name] = struct{}{}
	}
	addedNodes, removedNodes := w.replaceCurrentNodes(newNodes)
	w.emitNodeEvents(NodeAdded, addedNodes)
	w.emitNodeEvents(NodeRemoved, removedNodes)
	return nil
}

func (w *NodeWatcher) replaceCurrentNodes(
	newNodes map[string]struct{},
) ([]string, []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentNodes == nil {
		w.currentNodes = make(map[string]struct{})
	}
	addedNodes := make([]string, 0, len(newNodes))
	removedNodes := make([]string, 0, len(w.currentNodes))
	for nodeName := range newNodes {
		if _, tracked := w.currentNodes[nodeName]; !tracked {
			addedNodes = append(addedNodes, nodeName)
		}
	}
	for nodeName := range w.currentNodes {
		if _, exists := newNodes[nodeName]; !exists {
			removedNodes = append(removedNodes, nodeName)
		}
	}
	w.currentNodes = newNodes
	return addedNodes, removedNodes
}

func (w *NodeWatcher) emitNodeEvents(eventType NodeEventType, nodeNames []string) {
	for _, nodeName := range nodeNames {
		w.nodeHandler(NodeEvent{
			Type:     eventType,
			NodeName: nodeName,
		})
	}
}

func (w *NodeWatcher) consumeWatchEvents(
	ctx context.Context,
	watcher watch.Interface,
	logger zerolog.Logger,
) bool {
	watchChan := watcher.ResultChan()
	for {
		select {
		case <-ctx.Done():
			watcher.Stop()
			logger.Info().Msg("Node watcher shutting down")
			return false
		case event, ok := <-watchChan:
			if !ok {
				return true
			}
			if w.handleNodeWatchEvent(event, logger) {
				return true
			}
		}
	}
}

func (w *NodeWatcher) handleNodeWatchEvent(
	event watch.Event,
	logger zerolog.Logger,
) bool {
	if event.Type == watch.Error {
		if status, ok := event.Object.(*metav1.Status); ok && status.Code == 410 {
			logger.Warn().Msg("Node watch resource version expired, re-listing")
			w.lastResourceVersion = ""
			return true
		}
		logger.Error().Msg("Error watching nodes")
		return true
	}
	node, ok := event.Object.(*v1.Node)
	if !ok {
		logger.Warn().Msg("Unexpected object type in node watcher")
		return false
	}
	w.lastResourceVersion = node.ResourceVersion
	matchesSelector := w.nodeMatchesSelector(node)
	notifyAdd, notifyRemove := w.applyNodeWatchEvent(
		event.Type,
		node.Name,
		matchesSelector,
	)
	if notifyAdd {
		w.emitNodeEvents(NodeAdded, []string{node.Name})
	}
	if notifyRemove {
		w.emitNodeEvents(NodeRemoved, []string{node.Name})
	}
	return false
}

func (w *NodeWatcher) applyNodeWatchEvent(
	eventType watch.EventType,
	nodeName string,
	matchesSelector bool,
) (bool, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentNodes == nil {
		w.currentNodes = make(map[string]struct{})
	}
	_, wasTracked := w.currentNodes[nodeName]
	if eventType == watch.Deleted || (wasTracked && !matchesSelector) {
		if !wasTracked {
			return false, false
		}
		delete(w.currentNodes, nodeName)
		return false, true
	}
	if matchesSelector && !wasTracked {
		w.currentNodes[nodeName] = struct{}{}
		return true, false
	}
	return false, false
}

func (w *NodeWatcher) nodeMatchesSelector(node *v1.Node) bool {
	return w.parsedSelector.Matches(labels.Set(node.Labels))
}
