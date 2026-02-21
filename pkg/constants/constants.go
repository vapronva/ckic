package constants

import (
	"time"
)

const (
	DefaultNamespace             = "caddy-system"
	DefaultConfigMapName         = "caddy-config"
	DefaultCaddyDefaultConfigMap = "caddy-config"
	StateConfigMapName           = "ckic-state"
	StateKey                     = "state"
	ConfigUpdateDelay            = 10 * time.Second
	ConfigMapWatcherInitialDelay = 5 * time.Second
	ConfigMapWatcherMaxDelay     = 600 * time.Second
	NodeWatcherRetryInterval     = 1 * time.Minute
	CaddyAPIInitialDelay         = 5 * time.Second
	CaddyAPIMaxDelay             = 600 * time.Second
	CaddyAPIReadyTimeout         = 5 * time.Minute
)
