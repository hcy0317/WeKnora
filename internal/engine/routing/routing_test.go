package routing

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestResolveRewritesOnlyKnownManagedLocalEndpoints(t *testing.T) {
	t.Setenv(GatewayBaseURLEnv, "http://engine-gateway:18084/")

	tests := []struct {
		name     string
		group    lifecycle.Group
		endpoint string
		want     string
		managed  bool
	}{
		{
			name:     "paddle direct",
			group:    lifecycle.GroupPaddleOCR,
			endpoint: "http://paddleocr-vl:8080/",
			want:     "http://engine-gateway:18084/paddleocr",
			managed:  true,
		},
		{
			name:     "speaches openai base",
			group:    lifecycle.GroupASR,
			endpoint: "http://speaches:8000/v1",
			want:     "http://engine-gateway:18084/asr/v1",
			managed:  true,
		},
		{
			name:     "accelerator router",
			group:    lifecycle.GroupReranker,
			endpoint: "http://accelerator-router:18083/v1",
			want:     "http://engine-gateway:18084/reranker",
			managed:  true,
		},
		{
			name:     "external asr remains external",
			group:    lifecycle.GroupASR,
			endpoint: "https://api.example.com/v1",
			want:     "https://api.example.com/v1",
			managed:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, managed := Resolve(test.group, test.endpoint)
			require.Equal(t, test.want, got)
			require.Equal(t, test.managed, managed)
		})
	}
}

func TestResolveLeavesEndpointUntouchedWhenGatewayIsDisabledOrInvalid(t *testing.T) {
	for _, gatewayURL := range []string{"", "https://example.com", "http://engine-gateway:18084/path"} {
		t.Run(gatewayURL, func(t *testing.T) {
			t.Setenv(GatewayBaseURLEnv, gatewayURL)
			got, managed := Resolve(lifecycle.GroupASR, "http://speaches:8000/v1")
			require.Equal(t, "http://speaches:8000/v1", got)
			require.False(t, managed)
		})
	}
}
