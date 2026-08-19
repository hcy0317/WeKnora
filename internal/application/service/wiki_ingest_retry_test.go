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
