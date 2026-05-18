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
	LabelApp                     = "app"
	LabelInstance                = "instance"
	LabelCaddyManaged            = "ckic.cmld.ru/caddy-managed"
	LabelType                    = "ckic.cmld.ru/type"
	LabelAppValue                = "caddy"
	LabelManagedValue            = "true"
	LabelTypeAggregatedConfig    = "aggregated-config"
	LabelTypeImagePrePull        = "image-prepull"
	ManagedLabelSelector         = LabelCaddyManaged + "=" + LabelManagedValue
	PodNameEnvVar                = "CKIC_POD_NAME"
	NodeNameEnvVar               = "CKIC_NODE_NAME"
	PodIPEnvVar                  = "CKIC_POD_IP"
	VolumeNameCaddyConfig        = "caddy-config"
	VolumeNameData               = "opt-data"
	VolumeNameConfig             = "opt-config"
	HostLabelHostname            = "kubernetes.io/hostname"
	CiliumNodeLoadBalancerClass  = "io.cilium/node"
	CiliumNodeIPAMAnnotationKey  = "io.cilium.nodeipam/match-node-labels"
)

func InstanceLabelSelector(nodeName string) string {
	return LabelApp + "=" + LabelAppValue + "," + LabelInstance + "=" + nodeName
}

func AggregatedConfigLabels() map[string]string {
	return map[string]string{
		LabelCaddyManaged: LabelManagedValue,
		LabelType:         LabelTypeAggregatedConfig,
	}
}
