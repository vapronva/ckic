package caddy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"

	"git.horse/vapronva/ckic/pkg/constants"
)

type AdminAPIConfig struct {
	OriginKey string
	Client    *http.Client
}

func NewAdminAPIConfig(originKey string) *AdminAPIConfig {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &AdminAPIConfig{
		OriginKey: originKey,
		Client:    &http.Client{Timeout: 30 * time.Second, Transport: transport},
	}
}

func adminOrigin(originKey string) string {
	return fmt.Sprintf("http://%s.caddy-admin-api.ckic.cmld.ru", originKey)
}

func (i *Instance) UpdateConfig(ctx context.Context, configData string, apiConfig *AdminAPIConfig) error {
	if i.PodIP == "" {
		return errors.New("caddy pod IP is empty")
	}
	adminURL := "http://" + net.JoinHostPort(i.PodIP, strconv.Itoa(constants.CaddyAdminPort))
	body, err := i.postConfig(ctx, adminURL+"/adapt", "text/caddyfile", []byte(configData), apiConfig)
	if err != nil {
		return err
	}
	var adapted struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &adapted); err != nil {
		return fmt.Errorf("invalid Caddy adaptation response: %w", err)
	}
	if len(adapted.Result) == 0 || bytes.Equal(adapted.Result, []byte("null")) {
		return errors.New("caddy adaptation response has no result")
	}
	if _, err := i.postConfig(ctx, adminURL+"/load", "application/json", adapted.Result, apiConfig); err != nil {
		return err
	}
	log.Info().Str("node", i.NodeName).Str("pod", i.PodName).Msg("Updated Caddy configuration")
	return nil
}

func (i *Instance) postConfig(
	ctx context.Context,
	targetURL, contentType string,
	configData []byte,
	apiConfig *AdminAPIConfig,
) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(configData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	if req.URL.Path == "/load" {
		req.Header.Set("Cache-Control", "must-revalidate")
	}
	if apiConfig.OriginKey != "" {
		req.Header.Set("Origin", adminOrigin(apiConfig.OriginKey))
	}
	resp, err := apiConfig.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("caddy %s request failed: %w", req.URL.Path, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Warn().Err(err).Str("node", i.NodeName).Msg("Failed to close Caddy API response")
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read Caddy %s response: %w", req.URL.Path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("caddy %s returned HTTP %d: %s",
			req.URL.Path, resp.StatusCode, body[:min(len(body), 1024)])
	}
	return body, nil
}
