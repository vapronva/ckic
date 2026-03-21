package constants

import (
	"time"
)

const (
	StateConfigMapName           = "ckic-state"
	StateKey                     = "state"
	CaddyfileKey                 = "Caddyfile"
	ConfigMapWatcherInitialDelay = 5 * time.Second
	ConfigMapWatcherMaxDelay     = 600 * time.Second
)
