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
	CaddyAdminPort               = 2019
	CaddyAdminPortStr            = "2019"
	LabelApp                     = "app"
	LabelInstance                = "instance"
	LabelCaddyManaged            = "ckic.cmld.ru/caddy-managed"
	LabelAppValue                = "caddy"
	LabelManagedValue            = "true"
	ManagedLabelSelector         = LabelCaddyManaged + "=" + LabelManagedValue
)
