package adminclient

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
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
)

const (
	defaultBaseURL         = "https://host.docker.internal:18443"
	defaultCAFile          = "/run/weknora-engine-tls/ca.crt"
	defaultCertificateFile = "/run/weknora-engine-tls/client.crt"
	defaultPrivateKeyFile  = "/run/weknora-engine-tls/client.key"
	defaultTimeout         = 10 * time.Second
)

// Config contains the fixed local controller transport settings used by the
// WeKnora backend. Browser clients never receive these certificate paths.
type Config struct {
	BaseURL         string
	CAFile          string
	CertificateFile string
	PrivateKeyFile  string
	Timeout         time.Duration
}

// HTTPError preserves the controller status and current config revision so
// callers can map optimistic-lock conflicts without parsing error strings.
type HTTPError struct {
	StatusCode int
	Message    string
	Revision   uint64
}

func (e *HTTPError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("engine controller returned HTTP %d", e.StatusCode)
}

type Client struct {
	baseURL *url.URL
	http    *http.Client
}

func NewFromEnvironment() (*Client, error) {
	return New(Config{
		BaseURL:         envOrDefault("WEKNORA_ENGINE_CONTROLLER_URL", defaultBaseURL),
		CAFile:          envOrDefault("WEKNORA_ENGINE_CONTROLLER_CA", defaultCAFile),
		CertificateFile: envOrDefault("WEKNORA_ENGINE_CONTROLLER_CERT", defaultCertificateFile),
		PrivateKeyFile:  envOrDefault("WEKNORA_ENGINE_CONTROLLER_KEY", defaultPrivateKeyFile),
		Timeout:         defaultTimeout,
	})
}

func New(config Config) (*Client, error) {
	baseURL, err := url.Parse(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse engine controller URL: %w", err)
	}
	if baseURL.Scheme != "https" || baseURL.User != nil || baseURL.Host == "" ||
		baseURL.Path != "" || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, errors.New("engine controller URL must be an HTTPS origin")
	}
	if !isAllowedControllerHost(baseURL.Hostname()) {
		return nil, errors.New("engine controller URL must use a fixed local controller host")
	}

	caPEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read engine controller CA: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("engine controller CA contains no certificates")
	}
	certificate, err := tls.LoadX509KeyPair(config.CertificateFile, config.PrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load engine controller backend certificate: %w", err)
	}
	if config.Timeout <= 0 {
		config.Timeout = defaultTimeout
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			RootCAs:      rootCAs,
			Certificates: []tls.Certificate{certificate},
		},
	}
	return &Client{
		baseURL: baseURL,
		http: &http.Client{
			Transport: transport,
			Timeout:   config.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *Client) GetConfig(ctx context.Context) (*lifecycle.Config, error) {
	var config lifecycle.Config
	revision, err := c.doJSON(ctx, http.MethodGet, "/v1/config", 0, nil, &config)
	if err != nil {
		return nil, err
	}
	if revision == 0 || config.Revision != revision {
		return nil, errors.New("engine controller returned an inconsistent config revision")
	}
	return &config, nil
}

func (c *Client) UpdateConfig(
	ctx context.Context,
	expectedRevision uint64,
	config lifecycle.Config,
) (*lifecycle.Config, error) {
	var updated lifecycle.Config
	revision, err := c.doJSON(ctx, http.MethodPut, "/v1/config", expectedRevision, config, &updated)
	if err != nil {
		return nil, err
	}
	if revision == 0 || updated.Revision != revision {
		return nil, errors.New("engine controller returned an inconsistent config revision")
	}
	return &updated, nil
}

func (c *Client) GetGroupStatus(ctx context.Context, group lifecycle.Group) (lifecycle.GroupSnapshot, error) {
	if group != lifecycle.GroupPaddleOCR && group != lifecycle.GroupASR && group != lifecycle.GroupReranker {
		return lifecycle.GroupSnapshot{}, fmt.Errorf("unknown engine group %q", group)
	}
	var snapshot lifecycle.GroupSnapshot
	_, err := c.doJSON(ctx, http.MethodGet, "/v1/groups/"+string(group), 0, nil, &snapshot)
	return snapshot, err
}

func (c *Client) doJSON(
	ctx context.Context,
	method string,
	requestPath string,
	expectedRevision uint64,
	body any,
	target any,
) (uint64, error) {
	var requestBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("encode engine controller request: %w", err)
		}
		requestBody = bytes.NewReader(encoded)
	}
	requestURL := *c.baseURL
	requestURL.Path = requestPath
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), requestBody)
	if err != nil {
		return 0, fmt.Errorf("create engine controller request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if expectedRevision != 0 {
		request.Header.Set("If-Match", fmt.Sprintf(`"%d"`, expectedRevision))
	}

	response, err := c.http.Do(request)
	if err != nil {
		return 0, fmt.Errorf("contact engine controller: %w", err)
	}
	defer response.Body.Close()
	revision, _ := parseETag(response.Header.Get("ETag"))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		var payload struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&payload)
		return revision, &HTTPError{
			StatusCode: response.StatusCode,
			Message:    payload.Error,
			Revision:   revision,
		}
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return 0, fmt.Errorf("decode engine controller response: %w", err)
	}
	return revision, nil
}

func parseETag(value string) (uint64, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.HasPrefix(value, "W/") {
		return 0, errors.New("missing or weak engine controller ETag")
	}
	return strconv.ParseUint(strings.Trim(value, `"`), 10, 64)
}

func isAllowedControllerHost(host string) bool {
	switch strings.ToLower(host) {
	case "host.docker.internal", "localhost", "127.0.0.1":
		return true
	default:
		return false
	}
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
