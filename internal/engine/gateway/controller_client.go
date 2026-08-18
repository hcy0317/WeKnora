package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
)

var ErrControllerUnreachable = errors.New("engine controller is unreachable")

type ControllerClientConfig struct {
	BaseURL        string
	CACertPath     string
	CertPath       string
	KeyPath        string
	GatewayID      string
	RequestTimeout time.Duration
}

type ControllerClient struct {
	baseURL   *url.URL
	gatewayID string
	client    *http.Client
}

func NewControllerClient(config ControllerClientConfig) (*ControllerClient, error) {
	baseURL, err := url.Parse(config.BaseURL)
	if err != nil || baseURL.Scheme != "https" || baseURL.Host == "" || baseURL.User != nil ||
		baseURL.RawQuery != "" || baseURL.Fragment != "" || (baseURL.Path != "" && baseURL.Path != "/") {
		return nil, errors.New("controller base URL must be an HTTPS origin")
	}
	hostname := baseURL.Hostname()
	if hostname != "host.docker.internal" && hostname != "localhost" && hostname != "127.0.0.1" {
		return nil, fmt.Errorf("controller host %q is not allowed", hostname)
	}
	if config.GatewayID == "" {
		return nil, errors.New("gateway ID is required")
	}
	caPEM, err := os.ReadFile(config.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("read controller CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("controller CA file contains no certificates")
	}
	certificate, err := tls.LoadX509KeyPair(config.CertPath, config.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("load gateway controller client certificate: %w", err)
	}
	timeout := config.RequestTimeout
	if timeout <= 0 {
		timeout = 130 * time.Second
	}
	return &ControllerClient{
		baseURL:   baseURL,
		gatewayID: config.GatewayID,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{
				MinVersion:   tls.VersionTLS13,
				RootCAs:      roots,
				Certificates: []tls.Certificate{certificate},
				ServerName:   hostname,
			}},
		},
	}, nil
}

func (c *ControllerClient) Acquire(
	ctx context.Context,
	group lifecycle.Group,
	request lifecycle.AcquireRequest,
) (lifecycle.Lease, error) {
	request.GatewayID = c.gatewayID
	var lease lifecycle.Lease
	if err := c.doJSON(ctx, http.MethodPost, "/v1/groups/"+url.PathEscape(string(group))+"/leases", request, &lease); err != nil {
		return lifecycle.Lease{}, err
	}
	return lease, nil
}

func (c *ControllerClient) Renew(ctx context.Context, leaseID string) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/leases/"+url.PathEscape(leaseID)+"/renew", nil, nil)
}

func (c *ControllerClient) Release(
	ctx context.Context,
	leaseID string,
	reason lifecycle.ReleaseReason,
) error {
	path := "/v1/leases/" + url.PathEscape(leaseID) + "?reason=" + url.QueryEscape(string(reason))
	return c.doJSON(ctx, http.MethodDelete, path, nil, nil)
}

func (c *ControllerClient) Reconcile(ctx context.Context, reconcile lifecycle.GatewayReconcile) error {
	reconcile.GatewayID = c.gatewayID
	path := "/v1/gateways/" + url.PathEscape(c.gatewayID) + "/reconcile"
	return c.doJSON(ctx, http.MethodPost, path, reconcile, nil)
}

func (c *ControllerClient) doJSON(
	ctx context.Context,
	method string,
	path string,
	payload any,
	responsePayload any,
) error {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	requestURL := strings.TrimRight(c.baseURL.String(), "/") + path
	request, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return err
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrControllerUnreachable, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("engine controller returned %s: %s", response.Status, strings.TrimSpace(string(message)))
	}
	if responsePayload == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	if err := decoder.Decode(responsePayload); err != nil {
		return fmt.Errorf("decode engine controller response: %w", err)
	}
	return nil
}
