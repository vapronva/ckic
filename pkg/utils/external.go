package utils

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

type ExternalEndpointsMap map[string][]string

func ParseExternalEndpoints(endpoints []string, endpointsFile string) (ExternalEndpointsMap, error) {
	result := make(ExternalEndpointsMap)
	if endpointsFile != "" {
		fileData, err := os.ReadFile(endpointsFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read external endpoints file: %w", err)
		}
		if err := json.Unmarshal(fileData, &result); err != nil {
			return nil, fmt.Errorf("failed to parse external endpoints JSON: %w", err)
		}
		for nodeName, ips := range result {
			if strings.TrimSpace(nodeName) == "" {
				return nil, fmt.Errorf("node name cannot be empty in external endpoints file")
			}
			for _, ip := range ips {
				if !isValidIP(ip) {
					return nil, fmt.Errorf("invalid IP address in file for node %s: %s", nodeName, ip)
				}
			}
		}
	}
	for _, endpoint := range endpoints {
		parts := strings.SplitN(endpoint, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid external endpoint format: %s; expected format 'nodeName=ip1,ip2,...'", endpoint)
		}
		nodeName := strings.TrimSpace(parts[0])
		if nodeName == "" {
			return nil, fmt.Errorf("node name cannot be empty")
		}
		ipsRaw := strings.Split(parts[1], ",")
		var ips []string
		for _, ip := range ipsRaw {
			ip = strings.TrimSpace(ip)
			if !isValidIP(ip) {
				return nil, fmt.Errorf("invalid IP address format for node %s: %s", nodeName, ip)
			}
			ips = append(ips, ip)
		}
		result[nodeName] = append(result[nodeName], ips...)
	}
	return result, nil
}

func isValidIP(ip string) bool {
	return net.ParseIP(ip) != nil
}
