package controller

import (
	"sync"

	"github.com/rs/zerolog/log"

	"gl.vprw.ru/vapronva/ckic/pkg/caddy"
	"gl.vprw.ru/vapronva/ckic/pkg/watcher"
)

type WatcherCoordinator struct {
	mu                *sync.RWMutex
	notifyMu          sync.Mutex
	nodeWatcher       *watcher.NodeWatcher
	configWatcher     *watcher.ConfigWatcher
	deployedInstances map[string]*caddy.Instance
}

func NewWatcherCoordinator(nodeWatcher *watcher.NodeWatcher, configWatcher *watcher.ConfigWatcher,
	deployedInstances map[string]*caddy.Instance, mu *sync.RWMutex,
) *WatcherCoordinator {
	return &WatcherCoordinator{
		mu:                mu,
		nodeWatcher:       nodeWatcher,
		configWatcher:     configWatcher,
		deployedInstances: deployedInstances,
	}
}

func (wc *WatcherCoordinator) HasAvailableNodes() bool {
	wc.mu.RLock()
	defer wc.mu.RUnlock()
	return len(wc.deployedInstances) > 0
}

func (wc *WatcherCoordinator) NotifyNodeChange() {
	wc.notifyMu.Lock()
	defer wc.notifyMu.Unlock()
	wc.mu.RLock()
	hasNodes := len(wc.deployedInstances) > 0
	wc.mu.RUnlock()
	if hasNodes {
		wc.configWatcher.Resume()
	} else {
		wc.configWatcher.Pause()
	}
	log.Info().Msg("WatcherCoordinator notified node change")
}
