package watcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
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
	clientset     kubernetes.Interface
	labelSelector string
	nodeHandler   NodeHandler
	mu            sync.RWMutex
	currentNodes  map[string]bool
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
		return "", errors.New("node label selector is empty")
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
	return &NodeWatcher{
		clientset:     clientset,
		labelSelector: normalizedSelector,
		nodeHandler:   handler,
		currentNodes:  make(map[string]bool),
	}, nil
}

//nolint:gocognit,cyclop,funlen
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
			nodes, err := w.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
				LabelSelector: w.labelSelector,
			})
			if err != nil {
				logger.Error().Err(err).Msg("Failed to list nodes")
				time.Sleep(nodeListRetryDelay)
				continue
			}
			var addedNodes []string
			var removedNodes []string
			newNodes := make(map[string]bool, len(nodes.Items))
			for _, node := range nodes.Items {
				newNodes[node.Name] = true
			}
			w.mu.Lock()
			if w.currentNodes == nil {
				w.currentNodes = make(map[string]bool)
			}
			for nodeName := range newNodes {
				if !w.currentNodes[nodeName] {
					addedNodes = append(addedNodes, nodeName)
				}
			}
			for nodeName := range w.currentNodes {
				if !newNodes[nodeName] {
					removedNodes = append(removedNodes, nodeName)
				}
			}
			w.currentNodes = newNodes
			w.mu.Unlock()
			for _, nodeName := range addedNodes {
				w.nodeHandler(NodeEvent{
					Type:     NodeAdded,
					NodeName: nodeName,
				})
			}
			for _, nodeName := range removedNodes {
				w.nodeHandler(NodeEvent{
					Type:     NodeRemoved,
					NodeName: nodeName,
				})
			}
			watcher, err := w.clientset.CoreV1().Nodes().Watch(ctx, metav1.ListOptions{
				LabelSelector: w.labelSelector,
			})
			if err != nil {
				logger.Error().Err(err).Msg("Failed to create node watcher")
				time.Sleep(nodeListRetryDelay)
				continue
			}
			watchChan := watcher.ResultChan()
		watchLoop:
			for {
				select {
				case <-ctx.Done():
					watcher.Stop()
					logger.Info().Msg("Node watcher shutting down")
					return
				case event, ok := <-watchChan:
					if !ok {
						break watchLoop
					}
					if event.Type == watch.Error {
						logger.Error().Msg("Error watching nodes")
						break watchLoop
					}
					node, ok := event.Object.(*v1.Node)
					if !ok {
						logger.Warn().Msg("Unexpected object type in node watcher")
						continue
					}
					var notifyAdd bool
					var notifyRemove bool
					w.mu.Lock()
					if w.currentNodes == nil {
						w.currentNodes = make(map[string]bool)
					}
					wasTracked := w.currentNodes[node.Name]
					if event.Type == watch.Deleted {
						if wasTracked {
							delete(w.currentNodes, node.Name)
							notifyRemove = true
						}
						w.mu.Unlock()
						if notifyRemove {
							w.nodeHandler(NodeEvent{
								Type:     NodeRemoved,
								NodeName: node.Name,
							})
						}
						continue
					}
					if (event.Type == watch.Added || event.Type == watch.Modified) && !wasTracked {
						w.currentNodes[node.Name] = true
						notifyAdd = true
					}
					w.mu.Unlock()
					if notifyAdd {
						w.nodeHandler(NodeEvent{
							Type:     NodeAdded,
							NodeName: node.Name,
						})
					}
				}
			}
			logger.Info().Msg("Node watcher channel closed, restarting")
			time.Sleep(nodeWatchRetryDelay)
		}
	}
}
