package asr

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestConfigFromModel(t *testing.T) {
	m := &types.Model{
		ID:     "asr-1",
		Name:   "whisper-1",
		Source: types.ModelSourceRemote,
		Parameters: types.ModelParameters{
			BaseURL:       "https://api.example.com/v1",
			APIKey:        "sk",
			CustomHeaders: map[string]string{"X": "y"},
		},
	}
	cfg := ConfigFromModel(m)
	if cfg == nil || cfg.ModelID != "asr-1" || cfg.ModelName != "whisper-1" {
		t.Fatalf("identity mismatch: %+v", cfg)
	}
	if cfg.BaseURL != "https://api.example.com/v1" || cfg.APIKey != "sk" {
		t.Errorf("connection fields mismatch: %+v", cfg)
	}
	if cfg.CustomHeaders["X"] != "y" {
		t.Errorf("CustomHeaders not propagated: %+v", cfg.CustomHeaders)
	}
}

func TestConfigFromModel_Nil(t *testing.T) {
	if got := ConfigFromModel(nil); got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestConfigFromModelRoutesManagedSpeachesThroughEngineGateway(t *testing.T) {
	t.Setenv("WEKNORA_ENGINE_GATEWAY_URL", "http://engine-gateway:18084")
	model := &types.Model{
		ID:   "local-asr",
		Name: "Systran/faster-whisper-medium",
		Type: types.ModelTypeASR,
		Parameters: types.ModelParameters{
			BaseURL: "http://speaches:8000/v1",
		},
	}

	config := ConfigFromModel(model)
	if config.BaseURL != "http://engine-gateway:18084/asr/v1" {
		t.Fatalf("managed ASR base URL = %q", config.BaseURL)
	}
}
