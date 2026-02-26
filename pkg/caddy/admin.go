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

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
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
	resp, doErr := doAdminRequest(client, req)
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

func (i *Instance) UpdateConfig(
	ctx context.Context,
	configData string,
	method CommunicationMethod,
	apiConfig *AdminAPIConfig,
) error {
	logger := log.With().
		Str("node", i.NodeName).
		Str("pod", i.PodName).
		Str("method", fmt.Sprintf("%d", method)).
		Logger()
	logger.Debug().Msg("Updating Caddy configuration")
	adminURL, resolveErr := i.resolveAdminURL(ctx, method, logger)
	if resolveErr != nil {
		logger.Error().Err(resolveErr).Msg("Failed to resolve admin URL")
		return &errors.ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "failed to resolve admin URL",
			Err:      resolveErr,
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
	req, reqErr := i.buildConfigUpdateRequest(
		ctx,
		adminURL,
		configData,
		apiConfig,
		logger,
	)
	if reqErr != nil {
		logger.Error().Err(reqErr).Msg("Failed to create HTTP request")
		return &errors.ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "failed to create HTTP request",
			Err:      reqErr,
		}
	}
	resp, doErr := i.sendConfigUpdateRequest(req)
	if doErr != nil {
		logger.Error().Err(doErr).Msg("Failed to send configuration to Caddy")
		return &errors.ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "failed to send configuration",
			Err:      doErr,
		}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Warn().Err(closeErr).Msg("Failed to close response body")
		}
	}()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		logger.Error().Err(readErr).Msg("Failed to read response body")
		return &errors.ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "failed to read response",
			Err:      readErr,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := i.logConfigUpdateFailure(resp.StatusCode, body, logger)
		return &errors.ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "non-2xx response",
			Err:      err,
		}
	}
	logger.Info().Msg("Successfully updated Caddy configuration")
	return nil
}

func (i *Instance) resolveAdminURL(
	ctx context.Context,
	method CommunicationMethod,
	logger zerolog.Logger,
) (string, error) {
	switch method {
	case CommunicationMethodClusterIP:
		return clusterIPAdminURL(i.ServiceName, i.Namespace), nil
	case CommunicationMethodDirect:
		return i.directAdminURL(ctx, logger)
	case CommunicationMethodHostNetwork:
		return i.hostNetworkAdminURL(ctx, logger)
	default:
		return "", fmt.Errorf("unknown communication method: %d", method)
	}
}

func clusterIPAdminURL(serviceName, namespace string) string {
	return (&url.URL{
		Scheme: "http",
		Host: net.JoinHostPort(
			fmt.Sprintf("%s.%s.svc.cluster.local", serviceName, namespace),
			"2019",
		),
		Path: "/load",
	}).String()
}

func (i *Instance) directAdminURL(
	ctx context.Context,
	logger zerolog.Logger,
) (string, error) {
	pod, err := i.KubeClient.CoreV1().
		Pods(i.Namespace).
		Get(ctx, i.PodName, metav1.GetOptions{})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get pod information")
		return "", fmt.Errorf("failed to get pod info: %w", err)
	}
	podIP := pod.Status.PodIP
	if podIP == "" {
		return "", stdErrors.New("pod IP is empty")
	}
	return (&url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(podIP, "2019"),
		Path:   "/load",
	}).String(), nil
}

func (i *Instance) hostNetworkAdminURL(
	ctx context.Context,
	logger zerolog.Logger,
) (string, error) {
	node, err := i.KubeClient.CoreV1().
		Nodes().
		Get(ctx, i.NodeName, metav1.GetOptions{})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get node information")
		return "", fmt.Errorf("failed to get node info: %w", err)
	}
	nodeIP := preferredNodeIP(node)
	if nodeIP == "" {
		return "", stdErrors.New("no IP address found for node")
	}
	return (&url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(nodeIP, "2019"),
		Path:   "/load",
	}).String(), nil
}

func preferredNodeIP(node *corev1.Node) string {
	if node == nil {
		return ""
	}
	if internalIP := nodeAddressByType(node, "InternalIP"); internalIP != "" {
		return internalIP
	}
	return nodeAddressByType(node, "ExternalIP")
}

func nodeAddressByType(node *corev1.Node, addrType corev1.NodeAddressType) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == addrType {
			return addr.Address
		}
	}
	return ""
}

func (i *Instance) buildConfigUpdateRequest(
	ctx context.Context,
	adminURL string,
	configData string,
	apiConfig *AdminAPIConfig,
	logger zerolog.Logger,
) (*http.Request, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		adminURL,
		bytes.NewBufferString(configData),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/caddyfile")
	if apiConfig == nil || apiConfig.OriginKey == "" {
		return req, nil
	}
	req.Header.Set("Origin", adminOrigin(apiConfig.OriginKey))
	logger.Debug().
		Str("origin", req.Header.Get("Origin")).
		Msg("Added Origin header for admin API security")
	return req, nil
}

func (i *Instance) sendConfigUpdateRequest(req *http.Request) (*http.Response, error) {
	client := buildAdminHTTPClient(nil, configPushTimeout)
	return doAdminRequest(client, req)
}

func (i *Instance) logConfigUpdateFailure(
	statusCode int,
	body []byte,
	logger zerolog.Logger,
) error {
	err := fmt.Errorf("caddy returned status %d: %s", statusCode, string(body))
	if statusCode >= 400 && statusCode < 500 {
		logger.Error().
			Err(err).
			Int("status", statusCode).
			Msg("Caddy configuration update failed with client error")
		return err
	}
	logger.Error().
		Err(err).
		Int("status", statusCode).
		Msg("Caddy configuration update failed")
	return err
}

func doAdminRequest(client *http.Client, req *http.Request) (*http.Response, error) {
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	return transport.RoundTrip(req)
}

func buildAdminHTTPClient(apiConfig *AdminAPIConfig, timeout time.Duration) *http.Client {
	client := &http.Client{Timeout: timeout}
	_ = apiConfig
	return client
}
