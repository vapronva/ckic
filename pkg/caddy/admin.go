package caddy

import (
	"bytes"
	"context"
	stdErrors "errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"git.horse/vapronva/ckic/pkg/constants"
	"git.horse/vapronva/ckic/pkg/errors"
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

const (
	readinessRequestTimeout = 10 * time.Second
	configPushTimeout       = 30 * time.Second
)

func waitForCaddyAPIReady(
	ctx context.Context,
	adminURL string,
	apiConfig *AdminAPIConfig,
	client *http.Client,
) error {
	initialDelay := constants.CaddyAPIInitialDelay
	maxDelay := constants.CaddyAPIMaxDelay
	delay := initialDelay
	multiplier := 1.5
	readyURL := readinessURL(adminURL)
	client = readinessClient(client)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"context deadline exceeded while waiting for Caddy API: %w",
				ctx.Err(),
			)
		case <-time.After(delay):
		}
		ready, err := readinessProbe(ctx, client, readyURL, apiConfig, adminURL)
		if err != nil {
			log.Debug().
				Err(err).
				Str("adminURL", adminURL).
				Str("readyURL", readyURL).
				Msgf("Caddy Admin API readiness probe failed, retrying in %v", delay)
		} else if ready {
			return nil
		}
		if err == nil {
			log.Debug().
				Str("adminURL", adminURL).
				Str("readyURL", readyURL).
				Msgf("Caddy Admin API not ready, retrying in %v", delay)
		}
		delay = min(time.Duration(float64(delay)*multiplier), maxDelay)
	}
}

func readinessURL(adminURL string) string {
	if before, ok := strings.CutSuffix(adminURL, "/load"); ok {
		return before + "/config/"
	}
	return adminURL
}

func readinessClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return &http.Client{Timeout: readinessRequestTimeout}
}

func readinessProbe(
	ctx context.Context,
	client *http.Client,
	readyURL string,
	apiConfig *AdminAPIConfig,
	adminURL string,
) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, readyURL, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create readiness request: %w", err)
	}
	if apiConfig != nil && apiConfig.OriginKey != "" {
		req.Header.Set("Origin", adminOrigin(apiConfig.OriginKey))
	}
	//nolint:gosec
	resp, doErr := client.Do(req)
	if doErr != nil {
		return false, doErr
	}
	drainAndCloseReadinessBody(resp, adminURL, readyURL)
	return resp.StatusCode >= 200 && resp.StatusCode < 300, nil
}

func adminOrigin(originKey string) string {
	return fmt.Sprintf("http://%s.caddy-admin-api.ckic.cmld.ru", originKey)
}

func drainAndCloseReadinessBody(resp *http.Response, adminURL, readyURL string) {
	if resp == nil || resp.Body == nil {
		return
	}
	if _, copyErr := io.Copy(io.Discard, resp.Body); copyErr != nil {
		log.Warn().
			Err(copyErr).
			Str("adminURL", adminURL).
			Str("readyURL", readyURL).
			Msg("Failed to drain readiness response body")
	}
	if closeErr := resp.Body.Close(); closeErr != nil {
		log.Warn().
			Err(closeErr).
			Str("adminURL", adminURL).
			Str("readyURL", readyURL).
			Msg("Failed to close readiness response body")
	}
}

//nolint:gocognit,funlen
func (i *Instance) UpdateConfig(
	ctx context.Context,
	configData string,
	method CommunicationMethod,
	apiConfig *AdminAPIConfig,
) error {
	var adminURL string
	scheme := "http"
	port := "2019"
	logger := log.With().
		Str("node", i.NodeName).
		Str("pod", i.PodName).
		Str("method", fmt.Sprintf("%d", method)).
		Logger()
	logger.Debug().Msg("Updating Caddy configuration")
	switch method {
	case CommunicationMethodClusterIP:
		adminURL = (&url.URL{
			Scheme: scheme,
			Host: net.JoinHostPort(
				fmt.Sprintf("%s.%s.svc.cluster.local", i.ServiceName, i.Namespace),
				port,
			),
			Path: "/load",
		}).String()
	case CommunicationMethodDirect:
		pod, err := i.KubeClient.CoreV1().Pods(i.Namespace).Get(ctx, i.PodName, metav1.GetOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to get pod information")
			return &errors.ConfigurationFailedError{
				NodeName: i.NodeName,
				Reason:   "failed to get pod info",
				Err:      err,
			}
		}
		podIP := pod.Status.PodIP
		if podIP == "" {
			podIPErr := stdErrors.New("pod IP is empty")
			logger.Error().Err(podIPErr).Msg("Cannot get pod IP for direct communication")
			return &errors.ConfigurationFailedError{
				NodeName: i.NodeName,
				Reason:   "pod IP is empty",
				Err:      podIPErr,
			}
		}
		adminURL = (&url.URL{Scheme: scheme, Host: net.JoinHostPort(podIP, port), Path: "/load"}).String()
	case CommunicationMethodHostNetwork:
		node, err := i.KubeClient.CoreV1().Nodes().Get(ctx, i.NodeName, metav1.GetOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to get node information")
			return &errors.ConfigurationFailedError{
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
			nodeIPErr := stdErrors.New("no IP address found for node")
			logger.Error().Err(nodeIPErr).Msg("Cannot get node IP for hostNetwork communication")
			return &errors.ConfigurationFailedError{
				NodeName: i.NodeName,
				Reason:   "node IP not found",
				Err:      nodeIPErr,
			}
		}
		adminURL = (&url.URL{Scheme: scheme, Host: net.JoinHostPort(nodeIP, port), Path: "/load"}).String()
	default:
		err := fmt.Errorf("unknown communication method: %d", method)
		logger.Error().Err(err).Msg("Invalid communication method")
		return &errors.ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "unknown communication method",
			Err:      err,
		}
	}
	readinessClient := buildAdminHTTPClient(apiConfig, readinessRequestTimeout)
	readyCtx, cancel := context.WithTimeout(ctx, constants.CaddyAPIReadyTimeout)
	defer cancel()
	if readyErr := waitForCaddyAPIReady(
		readyCtx,
		adminURL,
		apiConfig,
		readinessClient,
	); readyErr != nil {
		logger.Error().Err(readyErr).Msg("Caddy Admin API not ready")
		return &errors.ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "admin API not ready",
			Err:      readyErr,
		}
	}
	client := buildAdminHTTPClient(apiConfig, configPushTimeout)
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		adminURL,
		bytes.NewBufferString(configData),
	)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create HTTP request")
		return &errors.ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "failed to create HTTP request",
			Err:      err,
		}
	}
	req.Header.Set("Content-Type", "text/caddyfile")
	if apiConfig != nil && apiConfig.OriginKey != "" {
		req.Header.Set(
			"Origin",
			fmt.Sprintf("http://%s.caddy-admin-api.ckic.cmld.ru", apiConfig.OriginKey),
		)
		logger.Debug().
			Str("origin", req.Header.Get("Origin")).
			Msg("Added Origin header for admin API security")
	}
	//nolint:gosec
	resp, err := client.Do(req)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to send configuration to Caddy")
		return &errors.ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "failed to send configuration",
			Err:      err,
		}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Warn().Err(closeErr).Msg("Failed to close response body")
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to read response body")
		return &errors.ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "failed to read response",
			Err:      err,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = fmt.Errorf("caddy returned status %d: %s", resp.StatusCode, string(body))
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			logger.Error().
				Err(err).
				Int("status", resp.StatusCode).
				Msg("Caddy configuration update failed with client error")
		} else {
			logger.Error().
				Err(err).
				Int("status", resp.StatusCode).
				Msg("Caddy configuration update failed")
		}
		return &errors.ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "non-2xx response",
			Err:      err,
		}
	}
	logger.Info().Msg("Successfully updated Caddy configuration")
	return nil
}

func buildAdminHTTPClient(apiConfig *AdminAPIConfig, timeout time.Duration) *http.Client {
	client := &http.Client{Timeout: timeout}
	_ = apiConfig
	return client
}
