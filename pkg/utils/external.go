package utils

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
)

type ExternalEndpointsMap map[string][]string

func ParseExternalEndpoints(endpoints []string, endpointsFile string) (ExternalEndpointsMap, error) {
	result := make(ExternalEndpointsMap)
	if endpointsFile != "" {
		cleanPath := filepath.Clean(endpointsFile)
		if !filepath.IsAbs(cleanPath) {
			return nil, fmt.Errorf("external endpoints file must be an absolute path: %s", endpointsFile)
		}
		fileData, err := os.ReadFile(cleanPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read external endpoints file: %w", err)
		}
		var rawResult map[string][]string
		if err := json.Unmarshal(fileData, &rawResult); err != nil {
			return nil, fmt.Errorf("failed to parse external endpoints JSON: %w", err)
		}
		for nodeName, ips := range rawResult {
			trimmedNodeName := strings.TrimSpace(nodeName)
			if trimmedNodeName == "" {
				return nil, fmt.Errorf("node name cannot be empty in external endpoints file")
			}
			for _, ip := range ips {
				if !isValidIP(ip) {
					return nil, fmt.Errorf("invalid IP address in file for node %s: %s", trimmedNodeName, ip)
				}
			}
			result[trimmedNodeName] = append(result[trimmedNodeName], ips...)
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
