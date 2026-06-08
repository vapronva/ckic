package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
)

type ExternalEndpointsMap map[string][]string

const endpointKVParts = 2

func ParseExternalEndpoints(
	endpoints []string,
	endpointsFile string,
) (ExternalEndpointsMap, error) {
	result := make(ExternalEndpointsMap)
	if endpointsFile != "" {
		if err := mergeExternalEndpointsFile(result, endpointsFile); err != nil {
			return nil, err
		}
	}
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

func mergeExternalEndpointsFile(result ExternalEndpointsMap, endpointsFile string) error {
	cleanPath := filepath.Clean(endpointsFile)
	if !filepath.IsAbs(cleanPath) {
		return fmt.Errorf(
			"external endpoints file must be an absolute path: %s",
			endpointsFile,
		)
	}
	fileData, err := os.ReadFile(cleanPath)
	if err != nil {
		return fmt.Errorf("failed to read external endpoints file: %w", err)
	}
	var rawResult map[string][]string
	if unmarshalErr := json.Unmarshal(fileData, &rawResult); unmarshalErr != nil {
		return fmt.Errorf("failed to parse external endpoints JSON: %w", unmarshalErr)
	}
	for nodeName, ips := range rawResult {
		trimmedNodeName := strings.TrimSpace(nodeName)
		if trimmedNodeName == "" {
			return errors.New("node name cannot be empty in external endpoints file")
		}
		updatedIPs, appendErr := appendUniqueValidatedIPs(
			result[trimmedNodeName],
			ips,
			func(ip string) error {
				return fmt.Errorf(
					"invalid IP address in file for node %s: %s",
					trimmedNodeName,
					ip,
				)
			},
		)
		if appendErr != nil {
			return appendErr
		}
		result[trimmedNodeName] = updatedIPs
	}
	return nil
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
