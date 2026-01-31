package caddy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"gl.vprw.ru/vapronva/ckic/pkg/constants"
	"gl.vprw.ru/vapronva/ckic/pkg/errors"
)

type CommunicationMethod int

const (
	CommunicationMethodClusterIP CommunicationMethod = iota
	CommunicationMethodDirect
	CommunicationMethodHostNetwork
)

type AdminAPIConfig struct {
	OriginKey string
}

func waitForCaddyAPIReady(ctx context.Context, adminURL string, apiConfig *AdminAPIConfig) error {
	initialDelay := constants.CaddyAPIInitialDelay
	maxDelay := constants.CaddyAPIMaxDelay
	delay := initialDelay
	multiplier := 1.5
	readyURL := adminURL
	if before, ok := strings.CutSuffix(adminURL, "/load"); ok {
		readyURL = before + "/config/"
	}
	client := &http.Client{Timeout: 10 * time.Second}
	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context deadline exceeded while waiting for Caddy API: %w", ctx.Err())
		case <-ticker.C:
			req, err := http.NewRequestWithContext(ctx, "GET", readyURL, nil)
			if err != nil {
				return fmt.Errorf("failed to create readiness request: %w", err)
			}
			if apiConfig != nil && apiConfig.OriginKey != "" {
				req.Header.Set("Origin", fmt.Sprintf("http://%s.caddy-admin-api.ckic.cmld.ru", apiConfig.OriginKey))
			}
			resp, err := client.Do(req)
			if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				_ = resp.Body.Close()
				return nil
			}
			if err == nil {
				_ = resp.Body.Close()
			}
			log.Debug().Str("adminURL", adminURL).Str("readyURL", readyURL).Msgf("Caddy Admin API not ready, retrying in %v", delay)
			delay = min(time.Duration(float64(delay)*multiplier), maxDelay)
			ticker.Reset(delay)
		}
	}
}

func (i *Instance) UpdateConfig(ctx context.Context, configData string, method CommunicationMethod, apiConfig *AdminAPIConfig) error {
	var adminURL string
	logger := log.With().Str("node", i.NodeName).Str("pod", i.PodName).Str("method", fmt.Sprintf("%d", method)).Logger()
	logger.Debug().Msg("Updating Caddy configuration")
	switch method {
	case CommunicationMethodClusterIP:
		adminURL = fmt.Sprintf("http://%s.%s.svc.cluster.local:2019/load", i.ServiceName, i.Namespace)
	case CommunicationMethodDirect:
		pod, err := i.KubeClient.CoreV1().Pods(i.Namespace).Get(ctx, i.PodName, metav1.GetOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to get pod information")
			return &errors.ErrConfigurationFailed{
				NodeName: i.NodeName,
				Reason:   "failed to get pod info",
				Err:      err,
			}
		}
		podIP := pod.Status.PodIP
		if podIP == "" {
			err := fmt.Errorf("pod IP is empty")
			logger.Error().Err(err).Msg("Cannot get pod IP for direct communication")
			return &errors.ErrConfigurationFailed{
				NodeName: i.NodeName,
				Reason:   "pod IP is empty",
				Err:      err,
			}
		}
		adminURL = fmt.Sprintf("http://%s:2019/load", podIP)
	case CommunicationMethodHostNetwork:
		node, err := i.KubeClient.CoreV1().Nodes().Get(ctx, i.NodeName, metav1.GetOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to get node information")
			return &errors.ErrConfigurationFailed{
				NodeName: i.NodeName,
				Reason:   "failed to get node info",
				Err:      err,
			}
		}
		var nodeIP string
		for _, addr := range node.Status.Addresses {
			if addr.Type == "InternalIP" {
				nodeIP = addr.Address
				break
			}
		}
		if nodeIP == "" {
			for _, addr := range node.Status.Addresses {
				if addr.Type == "ExternalIP" {
					nodeIP = addr.Address
					break
				}
			}
		}
		if nodeIP == "" {
			err := fmt.Errorf("no IP address found for node")
			logger.Error().Err(err).Msg("Cannot get node IP for hostNetwork communication")
			return &errors.ErrConfigurationFailed{
				NodeName: i.NodeName,
				Reason:   "node IP not found",
				Err:      err,
			}
		}
		adminURL = fmt.Sprintf("http://%s:2019/load", nodeIP)
	default:
		err := fmt.Errorf("unknown communication method: %d", method)
		logger.Error().Err(err).Msg("Invalid communication method")
		return &errors.ErrConfigurationFailed{
			NodeName: i.NodeName,
			Reason:   "unknown communication method",
			Err:      err,
		}
	}
	readyCtx, cancel := context.WithTimeout(ctx, constants.CaddyAPIReadyTimeout)
	defer cancel()
	if err := waitForCaddyAPIReady(readyCtx, adminURL, apiConfig); err != nil {
		logger.Error().Err(err).Msg("Caddy Admin API not ready")
		return &errors.ErrConfigurationFailed{
			NodeName: i.NodeName,
			Reason:   "admin API not ready",
			Err:      err,
		}
	}
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	// #nosec G107
	req, err := http.NewRequestWithContext(ctx, "POST", adminURL, bytes.NewBufferString(configData))
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create HTTP request")
		return &errors.ErrConfigurationFailed{
			NodeName: i.NodeName,
			Reason:   "failed to create HTTP request",
			Err:      err,
		}
	}
	req.Header.Set("Content-Type", "text/caddyfile")
	if apiConfig != nil && apiConfig.OriginKey != "" {
		req.Header.Set("Origin", fmt.Sprintf("http://%s.caddy-admin-api.ckic.cmld.ru", apiConfig.OriginKey))
		logger.Debug().Str("origin", req.Header.Get("Origin")).Msg("Added Origin header for admin API security")
	}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to send configuration to Caddy")
		return &errors.ErrConfigurationFailed{
			NodeName: i.NodeName,
			Reason:   "failed to send configuration",
			Err:      err,
		}
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to read response body")
		return &errors.ErrConfigurationFailed{
			NodeName: i.NodeName,
			Reason:   "failed to read response",
			Err:      err,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = fmt.Errorf("caddy returned status %d: %s", resp.StatusCode, string(body))
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			logger.Error().Err(err).Int("status", resp.StatusCode).Msg("Caddy configuration update failed with client error")
		} else {
			logger.Error().Err(err).Int("status", resp.StatusCode).Msg("Caddy configuration update failed")
		}
		return &errors.ErrConfigurationFailed{
			NodeName: i.NodeName,
			Reason:   "non-2xx response",
			Err:      err,
		}
	}
	logger.Info().Msg("Successfully updated Caddy configuration")
	return nil
}
