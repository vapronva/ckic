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
		updatedIPs, appendErr := appendUniqueValidatedIPs(
			result[nodeName],
			strings.Split(parts[1], ","),
			func(ip string) error {
				return fmt.Errorf(
					"invalid IP address format for node %s: %s",
					nodeName,
					ip,
				)
			},
		)
		if appendErr != nil {
			return nil, appendErr
		}
		result[nodeName] = updatedIPs
	}
	return result, nil
}

func appendUniqueValidatedIPs(
	existing []string,
	rawIPs []string,
	invalidIPError func(string) error,
) ([]string, error) {
	seen := make(map[string]struct{}, len(existing))
	for _, ip := range existing {
		seen[net.ParseIP(ip).String()] = struct{}{}
	}
	result := existing
	for _, rawIP := range rawIPs {
		ip := strings.TrimSpace(rawIP)
		parsed := net.ParseIP(ip)
		if parsed == nil {
			return nil, invalidIPError(ip)
		}
		canonical := parsed.String()
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		result = append(result, canonical)
	}
	return result, nil
}
