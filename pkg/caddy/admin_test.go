package caddy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestUpdateConfigLoadsAdaptedJSON(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, adaptation        string
		adaptStatus, loadStatus int
		wantLoads               int32
		wantError               bool
	}{
		{"success", `{"result":{"apps":{}},"warnings":[{"message":"warning"}]}`, 200, 200, 1, false},
		{"load failure", `{"result":{"apps":{}},"warnings":[{"message":"warning"}]}`, 200, 400, 1, true},
		{"adapt failure", `{"error":"bad config"}`, 400, 200, 0, true},
		{"missing result", `{"warnings":[]}`, 200, 200, 0, true},
		{"null result", `{"result":null}`, 200, 200, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var loads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.Header.Get("Origin") != "http://key.caddy-admin-api.ckic.cmld.ru" {
					t.Error("missing admin Origin")
				}
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Error(err)
				}
				switch request.URL.Path {
				case "/adapt":
					if request.Method != http.MethodPost || request.Header.Get("Content-Type") != "text/caddyfile" || string(body) != "config" {
						t.Errorf("unexpected adapt request: %s %s %q", request.Method, request.Header.Get("Content-Type"), body)
					}
					if request.Header.Get("Cache-Control") != "" {
						t.Error("adapt request forces reload")
					}
					w.WriteHeader(test.adaptStatus)
					_, _ = io.WriteString(w, test.adaptation)
				case "/load":
					loads.Add(1)
					if request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" || string(body) != `{"apps":{}}` {
						t.Errorf("unexpected load request: %s %s %q", request.Method, request.Header.Get("Content-Type"), body)
					}
					if request.Header.Get("Cache-Control") != "must-revalidate" {
						t.Error("load does not force reload")
					}
					w.WriteHeader(test.loadStatus)
				default:
					t.Errorf("unexpected admin path: %s", request.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(server.URL, "http://"))
			}}
			defer transport.CloseIdleConnections()
			api := NewAdminAPIConfig("key")
			api.Client.Transport = transport
			instance := &Instance{NodeName: "node1", PodIP: "192.0.2.1"}
			err := instance.UpdateConfig(t.Context(), "config", api)
			if (err != nil) != test.wantError {
				t.Fatalf("UpdateConfig = %v, want error=%v", err, test.wantError)
			}
			if loads.Load() != test.wantLoads {
				t.Fatalf("loads = %d, want %d", loads.Load(), test.wantLoads)
			}
			if test.loadStatus == http.StatusBadRequest && (err == nil || !strings.Contains(err.Error(), "400")) {
				t.Fatalf("load failure status was lost: %v", err)
			}
		})
	}
}
