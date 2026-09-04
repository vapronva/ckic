package caddy

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

func DeploymentName(nodeName string) string {
	return "caddy-" + nodeName
}

func loadBalancerServiceName(nodeName string) string {
	name := DeploymentName(nodeName) + "-loadbalancer"
	if len(validation.IsDNS1035Label(name)) == 0 {
		return name
	}
	prefix := strings.ReplaceAll(nodeName, ".", "-")
	prefix = strings.TrimRight(prefix[:min(len(prefix), 41)], "-")
	digest := sha256.Sum256([]byte(nodeName))
	return fmt.Sprintf("caddy-%s-%x-lb", prefix, digest[:6])
}

func prePullPodName(nodeName string) string {
	return "caddy-prepull-" + nodeName
}
