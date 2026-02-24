package watcher

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/constants"
)

type NodeEventType int

const (
	NodeAdded NodeEventType = iota
	NodeRemoved
)

const defaultLabelSelectorValue = "true"

const (
	labelSelectorParts  = 2
	nodeListRetryDelay  = 10 * time.Second
	nodeWatchRetryDelay = 5 * time.Second
)

type NodeEvent struct {
	Type     NodeEventType
	NodeName string
}

type NodeHandler func(NodeEvent)

type NodeWatcher struct {
	clientset    kubernetes.Interface
	labelKey     string
	labelValue   string
	nodeHandler  NodeHandler
	mu           sync.RWMutex
	currentNodes map[string]bool
}

func ParseLabelSelector(labelSelector string) (string, string) {
	selector := strings.TrimSpace(labelSelector)
	if selector == "" {
		return "", defaultLabelSelectorValue
	}
	var key string
	var value string
	switch {
	case strings.Contains(selector, "="):
		parts := strings.SplitN(selector, "=", labelSelectorParts)
		key = parts[0]
		value = parts[1]
	case strings.Contains(selector, ":"):
		parts := strings.SplitN(selector, ":", labelSelectorParts)
		key = parts[0]
		value = parts[1]
	default:
		key = selector
		value = defaultLabelSelectorValue
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "\"' ")
	if value == "" {
		value = defaultLabelSelectorValue
	}
	return key, value
}

func NewNodeWatcher(
	clientset kubernetes.Interface,
	labelSelector string,
	handler NodeHandler,
) *NodeWatcher {
	labelKey, labelValue := ParseLabelSelector(labelSelector)
	return &NodeWatcher{
		clientset:    clientset,
		labelKey:     labelKey,
		labelValue:   labelValue,
		nodeHandler:  handler,
		currentNodes: make(map[string]bool),
	}
}

func (w *NodeWatcher) snapshotTrackedNodes() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if len(w.currentNodes) == 0 {
		return nil
	}
	nodes := make([]string, 0, len(w.currentNodes))
	for nodeName := range w.currentNodes {
		nodes = append(nodes, nodeName)
	}
	return nodes
}

//nolint:gocognit,cyclop,funlen
func (w *NodeWatcher) Start(ctx context.Context) {
	logger := log.With().
		Str("component", "node_watcher").
		Str("label", w.labelKey+"="+w.labelValue).
		Logger()
	logger.Info().Msg("Starting node watcher")
	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Node watcher shutting down")
			return
		default:
			nodes, err := w.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
				LabelSelector: w.labelKey + "=" + w.labelValue,
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
				LabelSelector: w.labelKey + "=" + w.labelValue,
			})
			if err != nil {
				logger.Error().Err(err).Msg("Failed to create node watcher")
				time.Sleep(nodeListRetryDelay)
				continue
			}
			watchChan := watcher.ResultChan()
			retryTicker := time.NewTicker(constants.NodeWatcherRetryInterval)
		watchLoop:
			for {
				select {
				case <-ctx.Done():
					retryTicker.Stop()
					watcher.Stop()
					logger.Info().Msg("Node watcher shutting down")
					return
				case <-retryTicker.C:
					for _, nodeName := range w.snapshotTrackedNodes() {
						w.nodeHandler(NodeEvent{
							Type:     NodeAdded,
							NodeName: nodeName,
						})
					}
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
					hasLabel := node.Labels[w.labelKey] == w.labelValue
					if hasLabel && !wasTracked {
						w.currentNodes[node.Name] = true
						notifyAdd = true
					} else if !hasLabel && wasTracked {
						delete(w.currentNodes, node.Name)
						notifyRemove = true
					}
					w.mu.Unlock()
					if notifyAdd {
						w.nodeHandler(NodeEvent{
							Type:     NodeAdded,
							NodeName: node.Name,
						})
					}
					if notifyRemove {
						w.nodeHandler(NodeEvent{
							Type:     NodeRemoved,
							NodeName: node.Name,
						})
					}
				}
			}
			retryTicker.Stop()
			logger.Info().Msg("Node watcher channel closed, restarting")
			time.Sleep(nodeWatchRetryDelay)
		}
	}
}
