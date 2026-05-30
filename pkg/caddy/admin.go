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
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"git.horse/vapronva/ckic/pkg/constants"
)

type CommunicationMethod int

const (
	CommunicationMethodClusterIP CommunicationMethod = iota
	CommunicationMethodDirect
	CommunicationMethodHostNetwork
)

func (m CommunicationMethod) String() string {
	switch m {
	case CommunicationMethodClusterIP:
		return "clusterip"
	case CommunicationMethodDirect:
		return "direct"
	case CommunicationMethodHostNetwork:
		return "hostnetwork"
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

type AdminAPIConfig struct {
	OriginKey  string
	Readiness  *http.Client
	ConfigPush *http.Client
}

func NewAdminAPIConfig(originKey string) *AdminAPIConfig {
	transport := newAdminTransport()
	return &AdminAPIConfig{
		OriginKey:  originKey,
		Readiness:  &http.Client{Timeout: readinessRequestTimeout, Transport: transport},
		ConfigPush: &http.Client{Timeout: configPushTimeout, Transport: transport},
	}
}

const (
	readinessRequestTimeout     = 10 * time.Second
	configPushTimeout           = 30 * time.Second
	caddyAPIInitialDelay        = 5 * time.Second
	caddyAPIMaxDelay            = 600 * time.Second
	adminClientIdleConnsPerHost = 4
	adminClientIdleTimeout      = 60 * time.Second
	errorBodyLogMax             = 1024
)

func newAdminTransport() *http.Transport {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &http.Transport{
			MaxIdleConnsPerHost: adminClientIdleConnsPerHost,
			IdleConnTimeout:     adminClientIdleTimeout,
		}
	}
	t := base.Clone()
	t.MaxIdleConnsPerHost = adminClientIdleConnsPerHost
	t.IdleConnTimeout = adminClientIdleTimeout
	return t
}

func waitForCaddyAPIReady(
	ctx context.Context,
	adminURL string,
	apiConfig *AdminAPIConfig,
	client *http.Client,
	initialDelay time.Duration,
) error {
	const multiplier = 1.5
	delay := initialDelay
	readyURL := readinessURL(adminURL)
	first := true
	for {
		if first {
			first = false
			select {
			case <-ctx.Done():
				return fmt.Errorf(
					"context deadline exceeded while waiting for Caddy API: %w",
					ctx.Err(),
				)
			default:
			}
		} else {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return fmt.Errorf(
					"context deadline exceeded while waiting for Caddy API: %w",
					ctx.Err(),
				)
			case <-timer.C:
			}
			delay = min(time.Duration(float64(delay)*multiplier), caddyAPIMaxDelay)
		}
		ready, err := readinessProbe(ctx, client, readyURL, apiConfig, adminURL)
		if ready {
			return nil
		}
		log.Debug().
			Err(err).
			Str("adminURL", adminURL).
			Str("readyURL", readyURL).
			Msgf("Caddy Admin API not ready, retrying in %v", delay)
	}
}

func readinessURL(adminURL string) string {
	if before, ok := strings.CutSuffix(adminURL, "/load"); ok {
		return before + "/config/"
	}
	return adminURL
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

func (i *Instance) UpdateConfig(
	ctx context.Context,
	configData string,
	method CommunicationMethod,
	apiConfig *AdminAPIConfig,
) error {
	logger := log.With().
		Str("node", i.NodeName).
		Str("pod", i.PodName).
		Str("method", method.String()).
		Logger()
	logger.Debug().Msg("Updating Caddy configuration")
	adminURL, resolveErr := i.resolveAdminURL(ctx, method, logger)
	if resolveErr != nil {
		return &ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "failed to resolve admin URL",
			Err:      resolveErr,
		}
	}
	if readyErr := waitForCaddyAPIReady(
		ctx, adminURL, apiConfig, apiConfig.Readiness, caddyAPIInitialDelay,
	); readyErr != nil {
		return &ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "admin API not ready",
			Err:      readyErr,
		}
	}
	if validateErr := i.validateConfig(ctx, adminURL, configData, apiConfig, logger); validateErr != nil {
		return validateErr
	}
	resp, doErr := i.postCaddyfile(ctx, apiConfig.ConfigPush, adminURL, configData, apiConfig)
	if doErr != nil {
		return &ConfigurationFailedError{
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
		return &ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "failed to read response",
			Err:      readErr,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &ConfigurationFailedError{
			NodeName:   i.NodeName,
			Reason:     "non-2xx response",
			Err:        i.logCaddyAPIFailure("/load", resp.StatusCode, body, logger),
			StatusCode: resp.StatusCode,
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
		return clusterIPAdminURL(i.DeploymentName, i.Namespace), nil
	case CommunicationMethodDirect:
		return i.directAdminURL(ctx, logger)
	case CommunicationMethodHostNetwork:
		return i.hostNetworkAdminURL(ctx, logger)
	default:
		return "", fmt.Errorf("unknown communication method: %d", method)
	}
}

func clusterIPAdminURL(serviceName, namespace string) string {
	return adminLoadURL(fmt.Sprintf("%s.%s.svc.cluster.local", serviceName, namespace))
}

func adminLoadURL(host string) string {
	return (&url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(host, strconv.Itoa(constants.CaddyAdminPort)),
		Path:   "/load",
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
	if pod.Status.PodIP == "" {
		return "", stdErrors.New("pod IP is empty")
	}
	return adminLoadURL(pod.Status.PodIP), nil
}

func (i *Instance) hostNetworkAdminURL(
	ctx context.Context,
	logger zerolog.Logger,
) (string, error) {
	node, err := i.KubeClient.CoreV1().Nodes().Get(ctx, i.NodeName, metav1.GetOptions{})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get node information")
		return "", fmt.Errorf("failed to get node info: %w", err)
	}
	nodeIP := preferredNodeIP(node)
	if nodeIP == "" {
		return "", stdErrors.New("no IP address found for node")
	}
	return adminLoadURL(nodeIP), nil
}

func preferredNodeIP(node *corev1.Node) string {
	if node == nil {
		return ""
	}
	if internalIP := nodeAddressByType(node, corev1.NodeInternalIP); internalIP != "" {
		return internalIP
	}
	return nodeAddressByType(node, corev1.NodeExternalIP)
}

func nodeAddressByType(node *corev1.Node, addrType corev1.NodeAddressType) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == addrType {
			return addr.Address
		}
	}
	return ""
}

func (i *Instance) postCaddyfile(
	ctx context.Context,
	client *http.Client,
	targetURL, configData string,
	apiConfig *AdminAPIConfig,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, targetURL, bytes.NewBufferString(configData),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/caddyfile")
	if apiConfig != nil && apiConfig.OriginKey != "" {
		req.Header.Set("Origin", adminOrigin(apiConfig.OriginKey))
	}
	return client.Do(req)
}

func (i *Instance) validateConfig(
	ctx context.Context,
	adminURL, configData string,
	apiConfig *AdminAPIConfig,
	logger zerolog.Logger,
) error {
	resp, err := i.postCaddyfile(
		ctx, apiConfig.ConfigPush, adaptURL(adminURL), configData, apiConfig,
	)
	if err != nil {
		return &ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "failed to send configuration for validation",
			Err:      err,
		}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Warn().Err(closeErr).Msg("Failed to close validation response body")
		}
	}()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return &ConfigurationFailedError{
			NodeName: i.NodeName,
			Reason:   "failed to read validation response",
			Err:      readErr,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &ConfigurationFailedError{
			NodeName:   i.NodeName,
			Reason:     "configuration rejected by /adapt",
			Err:        i.logCaddyAPIFailure("/adapt", resp.StatusCode, body, logger),
			StatusCode: resp.StatusCode,
		}
	}
	logger.Debug().Msg("Configuration validated via /adapt")
	return nil
}

func adaptURL(adminURL string) string {
	if before, ok := strings.CutSuffix(adminURL, "/load"); ok {
		return before + "/adapt"
	}
	return adminURL
}

func (i *Instance) logCaddyAPIFailure(
	action string,
	statusCode int,
	body []byte,
	logger zerolog.Logger,
) error {
	truncated := body
	if len(truncated) > errorBodyLogMax {
		truncated = truncated[:errorBodyLogMax]
	}
	err := fmt.Errorf("caddy %s returned status %d: %s", action, statusCode, string(truncated))
	logger.Error().
		Err(err).
		Int("status", statusCode).
		Str("action", action).
		Msg("Caddy admin API request failed")
	return err
}
