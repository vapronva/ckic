package utils

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

type ExternalEndpointsMap map[string][]string

const endpointKVParts = 2

func ParseExternalEndpoints(endpoints []string) (ExternalEndpointsMap, error) {
	result := make(ExternalEndpointsMap)
	seen := make(map[string]map[string]struct{})
	for _, endpoint := range endpoints {
		parts := strings.SplitN(endpoint, "=", endpointKVParts)
		if len(parts) != endpointKVParts {
			return nil, fmt.Errorf(
				"invalid external endpoint format: %s; expected format 'nodeName=ip1,ip2,...'",
				endpoint,
			)
		}
		nodeName := strings.TrimSpace(parts[0])
		if nodeName == "" {
			return nil, errors.New("node name cannot be empty")
		}
		if seen[nodeName] == nil {
			seen[nodeName] = make(map[string]struct{})
		}
		for rawIP := range strings.SplitSeq(parts[1], ",") {
			ip := strings.TrimSpace(rawIP)
			parsed := net.ParseIP(ip)
			if parsed == nil {
				return nil, fmt.Errorf("invalid IP address format for node %s: %s", nodeName, ip)
			}
			canonical := parsed.String()
			if _, ok := seen[nodeName][canonical]; ok {
				continue
			}
			seen[nodeName][canonical] = struct{}{}
			result[nodeName] = append(result[nodeName], canonical)
		}
	}
	return result, nil
}
