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
	"slices"
	"strconv"
	"time"
)

const (
	adminPort           = 2019
	adminRequestTimeout = 30 * time.Second
	errorBodyLimit      = 1024
)

var (
	adminListen       = ":" + strconv.Itoa(adminPort)
	ErrConfigRejected = errors.New("configuration rejected")
)

type Admin struct {
	originKey   string
	forceReload bool
	client      *http.Client
}

func NewAdmin(originKey string, forceReload bool) *Admin {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &Admin{
		originKey:   originKey,
		forceReload: forceReload,
		client:      &http.Client{Timeout: adminRequestTimeout, Transport: transport},
	}
}

func adminOrigin(originKey string) string {
	return fmt.Sprintf("http://%s.caddy-admin-api.ckic.cmld.ru", originKey)
}

func (a *Admin) BootstrapCaddyfile() string {
	if a.originKey == "" {
		return fmt.Sprintf("{\n\tadmin %s\n}\n", adminListen)
	}
	return fmt.Sprintf("{\n\tadmin %s {\n\t\torigins %s\n\t\tenforce_origin\n\t}\n}\n", adminListen, adminOrigin(a.originKey))
}

func (a *Admin) Load(ctx context.Context, podIP, caddyfile string) error {
	adminURL := "http://" + net.JoinHostPort(podIP, strconv.Itoa(adminPort))
	adaptRequest, err := a.newRequest(ctx, adminURL+"/adapt", "text/caddyfile", []byte(caddyfile))
	if err != nil {
		return err
	}
	body, err := a.do(adaptRequest)
	if err != nil {
		return err
	}
	var adaptation struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &adaptation); err != nil {
		return fmt.Errorf("invalid Caddy adaptation response: %w", err)
	}
	if err := a.checkAdminReachable(adaptation.Result); err != nil {
		return err
	}
	loadRequest, err := a.newRequest(ctx, adminURL+"/load", "application/json", adaptation.Result)
	if err != nil {
		return err
	}
	if a.forceReload {
		loadRequest.Header.Set("Cache-Control", "must-revalidate")
	}
	_, err = a.do(loadRequest)
	return err
}

func (a *Admin) checkAdminReachable(config json.RawMessage) error {
	var adapted struct {
		Admin struct {
			Listen        string   `json:"listen"`
			EnforceOrigin bool     `json:"enforce_origin"`
			Origins       []string `json:"origins"`
		} `json:"admin"`
	}
	if err := json.Unmarshal(config, &adapted); err != nil {
		return fmt.Errorf("invalid adapted Caddy config: %w", err)
	}
	admin := adapted.Admin
	if admin.Listen != adminListen {
		return fmt.Errorf("%w: merged Caddyfile must keep the admin endpoint on %s, got %q", ErrConfigRejected, adminListen, admin.Listen)
	}
	if a.originKey == "" && admin.EnforceOrigin {
		return fmt.Errorf("%w: merged Caddyfile enforces an admin origin, but no origin key is configured", ErrConfigRejected)
	}
	if a.originKey != "" && !slices.Contains(admin.Origins, adminOrigin(a.originKey)) {
		return fmt.Errorf("%w: merged Caddyfile must allow the admin origin %s", ErrConfigRejected, adminOrigin(a.originKey))
	}
	return nil
}

func (a *Admin) newRequest(ctx context.Context, targetURL, contentType string, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	if a.originKey != "" {
		req.Header.Set("Origin", adminOrigin(a.originKey))
	}
	return req, nil
}

func (a *Admin) do(req *http.Request) ([]byte, error) {
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("caddy %s request failed: %w", req.URL.Path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read Caddy %s response: %w", req.URL.Path, err)
	}
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return body, nil
	}
	failure := fmt.Sprintf("caddy %s returned HTTP %d: %s", req.URL.Path, resp.StatusCode, body[:min(len(body), errorBodyLimit)])
	if resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError {
		return nil, fmt.Errorf("%w: %s", ErrConfigRejected, failure)
	}
	return nil, errors.New(failure)
}
