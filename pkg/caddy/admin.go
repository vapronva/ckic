package caddy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"gl.vprw.ru/vapronva/ckic/pkg/errors"
)

type CommunicationMethod int

const (
	CommunicationMethodClusterIP CommunicationMethod = iota
	CommunicationMethodDirect
)

func (i *Instance) UpdateConfig(configData string, method CommunicationMethod) error {
	var adminURL string
	logger := log.With().
		Str("node", i.NodeName).
		Str("pod", i.PodName).
		Str("method", fmt.Sprintf("%d", method)).
		Logger()
	logger.Debug().Msg("Updating Caddy configuration")
	switch method {
	case CommunicationMethodClusterIP:
		adminURL = fmt.Sprintf("http://%s.%s.svc.cluster.local:2019/load", i.ServiceName, i.Namespace)
	case CommunicationMethodDirect:
		pod, err := i.KubeClient.CoreV1().Pods(i.Namespace).Get(context.Background(), i.PodName, metav1.GetOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to get pod information")
			i.FailureCount++
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
			i.FailureCount++
			return &errors.ErrConfigurationFailed{
				NodeName: i.NodeName,
				Reason:   "pod IP is empty",
				Err:      err,
			}
		}
		adminURL = fmt.Sprintf("http://%s:2019/load", podIP)
	default:
		err := fmt.Errorf("unknown communication method: %d", method)
		logger.Error().Err(err).Msg("Invalid communication method")
		i.FailureCount++
		return &errors.ErrConfigurationFailed{
			NodeName: i.NodeName,
			Reason:   "unknown communication method",
			Err:      err,
		}
	}
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	req, err := http.NewRequest("POST", adminURL, bytes.NewBufferString(configData))
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create HTTP request")
		i.FailureCount++
		return &errors.ErrConfigurationFailed{
			NodeName: i.NodeName,
			Reason:   "failed to create HTTP request",
			Err:      err,
		}
	}
	req.Header.Set("Content-Type", "text/caddyfile")
	resp, err := client.Do(req)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to send configuration to Caddy")
		i.FailureCount++
		return &errors.ErrConfigurationFailed{
			NodeName: i.NodeName,
			Reason:   "failed to send configuration",
			Err:      err,
		}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to read response body")
		i.FailureCount++
		return &errors.ErrConfigurationFailed{
			NodeName: i.NodeName,
			Reason:   "failed to read response",
			Err:      err,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = fmt.Errorf("caddy returned status %d: %s", resp.StatusCode, string(body))
		logger.Error().Err(err).Int("status", resp.StatusCode).Msg("Caddy configuration update failed")
		i.FailureCount++
		return &errors.ErrConfigurationFailed{
			NodeName: i.NodeName,
			Reason:   "non-2xx response",
			Err:      err,
		}
	}
	logger.Info().Msg("Successfully updated Caddy configuration")
	i.FailureCount = 0
	return nil
}
