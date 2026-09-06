package utils

import (
	"fmt"
	"net"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation"
)

type ExternalEndpointsMap map[string][]string

func ParseExternalEndpoints(endpoints []string) (ExternalEndpointsMap, error) {
	result := make(ExternalEndpointsMap)
	for _, endpoint := range endpoints {
		nodeName, addresses, ok := strings.Cut(endpoint, "=")
		if !ok {
			return nil, fmt.Errorf(
				"--external-endpoints: invalid endpoint %q; expected format 'nodeName=ip1,ip2,...'",
				endpoint,
			)
		}
		nodeName = strings.TrimSpace(nodeName)
		if errs := content.IsDNS1123Subdomain(nodeName); len(errs) > 0 {
			return nil, fmt.Errorf("--external-endpoints: invalid Kubernetes node name %q: %s", nodeName, strings.Join(errs, "; "))
		}
		if errs := validation.IsValidLabelValue(nodeName); len(errs) > 0 {
			return nil, fmt.Errorf("--external-endpoints: node %q cannot be used as an instance label: %s", nodeName, strings.Join(errs, "; "))
		}
		for rawIP := range strings.SplitSeq(addresses, ",") {
			ip := strings.TrimSpace(rawIP)
			parsed := net.ParseIP(ip)
			if parsed == nil {
				return nil, fmt.Errorf("--external-endpoints: invalid IP address %q for node %q", ip, nodeName)
			}
			if !parsed.IsGlobalUnicast() {
				return nil, fmt.Errorf("--external-endpoints: IP address %q for node %q must be unicast and cannot be unspecified, loopback or link-local", ip, nodeName)
			}
			canonical := parsed.String()
			if slices.Contains(result[nodeName], canonical) {
				continue
			}
			result[nodeName] = append(result[nodeName], canonical)
		}
	}
	return result, nil
}
