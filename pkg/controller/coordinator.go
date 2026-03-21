package controller

import (
	"sync"

	"github.com/rs/zerolog/log"

	"git.horse/vapronva/ckic/pkg/caddy"
)

type WatcherCoordinator struct {
	mu                *sync.RWMutex
	notifyMu          sync.Mutex
	configWatcher     coordinatedConfigWatcher
	deployedInstances map[string]*caddy.Instance
}

type coordinatedConfigWatcher interface {
	EnsureSync()
	Pause()
}

func NewWatcherCoordinator(
	configWatcher coordinatedConfigWatcher,
	deployedInstances map[string]*caddy.Instance,
	mu *sync.RWMutex,
) *WatcherCoordinator {
	return &WatcherCoordinator{
		mu:                mu,
		configWatcher:     configWatcher,
		deployedInstances: deployedInstances,
	}
}

func (wc *WatcherCoordinator) NotifyNodeChange() {
	wc.notifyMu.Lock()
	defer wc.notifyMu.Unlock()
	wc.mu.RLock()
	hasNodes := len(wc.deployedInstances) > 0
	wc.mu.RUnlock()
	if hasNodes {
		wc.configWatcher.EnsureSync()
	} else {
		wc.configWatcher.Pause()
	}
	log.Info().Msg("WatcherCoordinator notified node change")
}
