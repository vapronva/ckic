package caddy

import "strings"

func DeploymentName(nodeName string) string {
	return "caddy-" + nodeResourceName(nodeName)
}

func loadBalancerServiceName(nodeName string) string {
	return DeploymentName(nodeName) + "-lb"
}

func prePullPodName(nodeName string) string {
	return "ckic-prepull-" + nodeResourceName(nodeName)
}

func nodeResourceName(nodeName string) string {
	return strings.ReplaceAll(nodeName, ".", "-")
}
