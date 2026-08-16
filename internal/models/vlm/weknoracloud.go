package vlm

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/models/openaiapi"
)

// WeKnoraCloudVLM implements VLM via the WeKnoraCloud API.
type WeKnoraCloudVLM struct {
	modelName                string
	remoteModelName          string
	modelID                  string
	appID                    string
	apiKey                   string
	baseURL                  string
	client                   *http.Client
	reasoningEffort          string
	configurationFingerprint string
}

// NewWeKnoraCloudVLM creates a WeKnoraCloud-backed VLM instance.
func NewWeKnoraCloudVLM(config *Config) (*WeKnoraCloudVLM, error) {
	if config.AppID == "" {
		return nil, fmt.Errorf("WeKnoraCloud VLM: AppID is required")
	}
	if config.AppSecret == "" {
		return nil, fmt.Errorf("WeKnoraCloud VLM: AppSecret is required")
	}
	baseURL := strings.TrimRight(config.BaseURL, "/")
	if err := validateVLMBaseURL(baseURL); err != nil {
		return nil, err
	}
	remoteModelName := ""
	reasoningEffort := ""
	if config.Extra != nil {
		if v, ok := config.Extra["remote_model_name"]; ok {
			if vs, ok := v.(string); ok {
				remoteModelName = strings.TrimSpace(vs)
			}
		}
		if v, ok := config.Extra["reasoning_effort"].(string); ok {
			reasoningEffort = strings.TrimSpace(v)
		}
	}
	return &WeKnoraCloudVLM{
		modelName:       config.ModelName,
		remoteModelName: remoteModelName,
		modelID:         config.ModelID,
		appID:           config.AppID,
		apiKey:          config.AppSecret,
		baseURL:         baseURL,
		client:          newVLMHTTPClient(vlmHTTPTimeout()),
		reasoningEffort: reasoningEffort,
		configurationFingerprint: openaiapi.SavedModelConfigFingerprint(openaiapi.SavedModelConfig{
			Provider: config.Provider, InterfaceType: normalizedVLMInterface(config.InterfaceType),
			ExtraConfig: config.Extra, Headers: config.CustomHeaders,
			Auth: map[string]string{"app_id": config.AppID, "app_secret": config.AppSecret},
		}),
	}, nil
}

type weKnoraCloudVLMContentPart struct {
	Type     string                   `json:"type"`
	Text     string                   `json:"text,omitempty"`
	ImageURL *weKnoraCloudVLMImageURL `json:"image_url,omitempty"`
}

type weKnoraCloudVLMImageURL struct {
	URL string `json:"url"`
}

type weKnoraCloudVLMMessage struct {
	Role    string      `json:"role"`
	Content interface{} `json:"content"`
}

type weKnoraCloudVLMRequest struct {
	Model               string                   `json:"model"`
	Messages            []weKnoraCloudVLMMessage `json:"messages"`
	MaxTokens           int                      `json:"max_tokens,omitempty"`
	MaxCompletionTokens int                      `json:"max_completion_tokens,omitempty"`
	Temperature         float64                  `json:"temperature,omitempty"`
	ReasoningEffort     string                   `json:"reasoning_effort,omitempty"`
	Stream              bool                     `json:"stream"`
}

// Predict sends images with a text prompt to the WeKnoraCloud API.
func (v *WeKnoraCloudVLM) Predict(ctx context.Context, imgBytesList [][]byte, prompt string) (string, error) {
	var parts []weKnoraCloudVLMContentPart

	parts = append(parts, weKnoraCloudVLMContentPart{
		Type: "text",
		Text: prompt,
	})

	for i, imgBytes := range imgBytesList {
		if len(imgBytes) > 0 {
			mimeType, err := detectImageMIME(imgBytes)
			if err != nil {
				return "", fmt.Errorf("WeKnoraCloud VLM image %d: %w", i, err)
			}
			b64 := base64.StdEncoding.EncodeToString(imgBytes)
			dataURI := fmt.Sprintf("data:%s;base64,%s", mimeType, b64)
			parts = append(parts, weKnoraCloudVLMContentPart{
				Type: "image_url",
				ImageURL: &weKnoraCloudVLMImageURL{
					URL: dataURI,
				},
			})
		}
	}

	effectiveModelName := v.effectiveModelName()
	reqBody := weKnoraCloudVLMRequest{
		Model: effectiveModelName,
		Messages: []weKnoraCloudVLMMessage{
			{
				Role:    "user",
				Content: parts,
			},
		},
		ReasoningEffort: v.reasoningEffort,
		Stream:          false,
	}
	reqBody.MaxTokens = defaultMaxToks
	reqBody.Temperature = float64(defaultTemp)

	totalImageSize := 0
	for _, img := range imgBytesList {
		totalImageSize += len(img)
	}
	logger.Infof(ctx, "[VLM] Calling WeKnoraCloud API, model=%s, baseURL=%s, numImages=%d, totalImageSize=%d",
		effectiveModelName, v.baseURL, len(imgBytesList), totalImageSize)

	content, err := v.predictWithNegotiatedProtocol(ctx, reqBody)
	if err != nil {
		return "", err
	}
	logger.Infof(ctx, "[VLM] WeKnoraCloud response received, len=%d", len(content))
	return content, nil
}

func (v *WeKnoraCloudVLM) effectiveModelName() string {
	if v.remoteModelName != "" {
		return v.remoteModelName
	}
	return v.modelName
}

func (v *WeKnoraCloudVLM) GetModelName() string { return v.modelName }
func (v *WeKnoraCloudVLM) GetModelID() string   { return v.modelID }
