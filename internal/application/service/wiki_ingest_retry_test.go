package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"syscall"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/openaiapi"
)

func TestIsTransientLLMError_HTTPStatuses(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		code int
		want bool
	}{
		{http.StatusGatewayTimeout, true},
		{http.StatusServiceUnavailable, true},
		{http.StatusBadGateway, true},
		{http.StatusInternalServerError, true},
		{http.StatusTooManyRequests, true},
		{http.StatusRequestTimeout, true},
		{520, true},
		{524, true},
		{http.StatusBadRequest, false},
		{http.StatusUnauthorized, false},
		{http.StatusForbidden, false},
		{http.StatusNotFound, false},
	}
	for _, tc := range cases {
		err := openaiapi.NewProtocolHTTPError(openaiapi.ProtocolResponses, tc.code, "provider response")
		got := isTransientLLMError(ctx, err)
		if got != tc.want {
			t.Errorf("isTransientLLMError(status %d) = %v, want %v", tc.code, got, tc.want)
		}
	}
	if isTransientLLMError(ctx, errors.New("API request failed with status 503: words only")) {
		t.Fatal("status-like free text must fail closed")
	}
}

func TestIsTransientLLMError_TypedTransportErrors(t *testing.T) {
	ctx := context.Background()
	for _, err := range []error{
		testWikiNetError{},
		io.ErrUnexpectedEOF,
		syscall.ECONNRESET,
		syscall.ECONNREFUSED,
		syscall.EPIPE,
		context.DeadlineExceeded,
	} {
		if !isTransientLLMError(ctx, err) {
			t.Errorf("typed transport error should be transient: %v", err)
		}
	}
	for _, message := range []string{
		"send request: connection reset by peer",
		"send request: tls handshake timeout",
		"stream_read_error",
		"model not configured for tool use",
	} {
		if isTransientLLMError(ctx, errors.New(message)) {
			t.Errorf("ambiguous free text should fail closed: %q", message)
		}
	}
}

func TestIsTransientLLMError_AbortsWhenParentCtxDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := openaiapi.NewProtocolHTTPError(openaiapi.ProtocolResponses, http.StatusGatewayTimeout, "Remote error")
	if isTransientLLMError(ctx, err) {
		t.Fatal("cancelled ctx should short-circuit to non-transient")
	}
}

func TestIsTransientLLMError_NilError(t *testing.T) {
	if isTransientLLMError(context.Background(), nil) {
		t.Fatal("nil error should not be transient")
	}
}

// TestIsTransientLLMError_RateLimit403 covers gateways that report QPM/QPS
// throttling as HTTP 403 with a rate-limit body instead of 429 (e.g. a MaaS
// gateway returning code 0x04030020, "调用频率（qpm）超限"). These carry the
// provider-embedded response body "API request failed with status 403: {...}",
// and only body-rated limit wording classifies them as transient — a plain
// authorization 403 must stay permanent.
func TestIsTransientLLMError_RateLimit403(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		msg  string
		want bool
	}{
		{"qpm gateway", `API request failed with status 403: {"code":"0x04030020","message":"调用频率（qpm）超限"}`, true},
		{"qps gateway", `API request failed with status 403: {"message":"qps exceeded"}`, true},
		{"rate limit", "API request failed with status 403: rate limit reached", true},
		{"too many requests", "API request failed with status 403: too many requests", true},
		{"throttled", "API request failed with status 403: request throttled", true},
		{"chinese frequency", "API request failed with status 403: 请求过于频繁，请稍后重试", true},
		{"busy", "API request failed with status 403: 服务繁忙", true},
		{"try again later", "API request failed with status 403: please try again later", true},
		{"slow down", "API request failed with status 403: slow down your requests", true},
		// Authorization-shaped 403s stay permanent.
		{"unauthorized", "API request failed with status 403: invalid api key", false},
		{"forbidden", "API request failed with status 403: forbidden", false},
		{"permission denied", "API request failed with status 403: permission denied", false},
		{"quota exhausted", "API request failed with status 403: forbidden (quota exhausted)", false},
		{"unknown body", `API request failed with status 403: {"error":"something else"}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTransientLLMError(ctx, errors.New(tc.msg)); got != tc.want {
				t.Errorf("isTransientLLMError(%q) = %v, want %v", tc.msg, got, tc.want)
			}
		})
	}
}
