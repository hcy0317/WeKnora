package ollama

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/ollama/ollama/api"
)

// OllamaService manages Ollama service
type OllamaService struct {
	client      *api.Client
	baseURL     string
	mu          sync.Mutex
	isAvailable bool
}

func ollamaEndpointLabel(parsedURL *url.URL) string {
	if parsedURL == nil {
		return "invalid"
	}
	return (&url.URL{Scheme: parsedURL.Scheme, Host: parsedURL.Host}).String()
}

// GetOllamaService gets Ollama service instance (singleton pattern)
func GetOllamaService() (*OllamaService, error) {
	// Get Ollama base URL from environment variable, if not set use the local default.
	baseURL := "http://localhost:11434"
	envURL := os.Getenv("OLLAMA_BASE_URL")
	if envURL != "" {
		baseURL = envURL
	}

	// Create URL object
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, errors.New("invalid Ollama service URL")
	}
	endpoint := ollamaEndpointLabel(parsedURL)
	logger.GetLogger(context.Background()).Infof("Ollama endpoint: %s", endpoint)

	// Dedicated HTTP client for Ollama instead of http.DefaultClient.
	// - Dial timeout prevents hanging when Ollama process is down or port unreachable
	// - No overall Timeout so long-running streaming calls are controlled by context cancellation
	ollamaHTTPClient := &http.Client{
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
			IdleConnTimeout:     90 * time.Second,
			MaxIdleConns:        10,
		},
	}
	client := api.NewClient(parsedURL, ollamaHTTPClient)

	if os.Getenv("OLLAMA_OPTIONAL") == "true" {
		logger.GetLogger(context.Background()).Info(
			"Ollama is optional for application bootstrap; runtime provider operations still fail closed",
		)
	}

	service := &OllamaService{
		client:  client,
		baseURL: baseURL,
	}

	return service, nil
}

func (s *OllamaService) checkAvailability(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.client.Heartbeat(ctx)
	if err != nil {
		s.isAvailable = false
		return fmt.Errorf("ollama service unavailable: %w", err)
	}

	s.isAvailable = true
	return nil
}

// StartService performs a real provider health check. Bootstrap code may decide
// whether that error is fatal, but runtime handlers must never receive a false
// success merely because OLLAMA_OPTIONAL was configured.
func (s *OllamaService) StartService(ctx context.Context) error {
	return s.checkAvailability(ctx)
}

// IsAvailable returns whether the service is available
func (s *OllamaService) IsAvailable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isAvailable
}

// IsModelAvailable checks if a model is available
func (s *OllamaService) IsModelAvailable(ctx context.Context, modelName string) (bool, error) {
	if err := s.checkAvailability(ctx); err != nil {
		return false, err
	}

	// Get model list
	listResp, err := s.client.List(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get model list: %w", err)
	}

	// If no version is specified for the model, add ":latest" by default
	checkModelName := modelName
	if !strings.Contains(modelName, ":") {
		checkModelName = modelName + ":latest"
	}
	// Check if model is in the list
	for _, model := range listResp.Models {
		if model.Name == checkModelName {
			return true, nil
		}
	}

	return false, nil
}

// PullModel pulls a model
func (s *OllamaService) PullModel(ctx context.Context, modelName string) error {
	// Check if model already exists
	available, err := s.IsModelAvailable(ctx, modelName)
	if err != nil {
		return err
	}
	if available {
		logger.GetLogger(ctx).Infof("Model %s already exists", modelName)
		return nil
	}

	// Use official client to pull model
	pullReq := &api.PullRequest{
		Name: modelName,
	}

	err = s.client.Pull(ctx, pullReq, func(progress api.ProgressResponse) error {
		if progress.Status != "" {
			if progress.Total > 0 && progress.Completed > 0 {
				percentage := float64(progress.Completed) / float64(progress.Total) * 100
				logger.GetLogger(ctx).Infof("Pull progress: %s (%.2f%%)",
					progress.Status, percentage)
			} else {
				logger.GetLogger(ctx).Infof("Pull status: %s", progress.Status)
			}
		}

		if progress.Total > 0 && progress.Completed == progress.Total {
			logger.GetLogger(ctx).Infof("Model %s pull completed", modelName)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to pull model: %w", err)
	}

	return nil
}

// EnsureModelAvailable ensures the model is available, pulls it if not available
func (s *OllamaService) EnsureModelAvailable(ctx context.Context, modelName string) error {
	available, err := s.IsModelAvailable(ctx, modelName)
	if err != nil {
		return err
	}

	if !available {
		return s.PullModel(ctx, modelName)
	}

	return nil
}

// GetVersion gets Ollama version
func (s *OllamaService) GetVersion(ctx context.Context) (string, error) {
	if err := s.checkAvailability(ctx); err != nil {
		return "", err
	}

	version, err := s.client.Version(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get Ollama version: %w", err)
	}
	return version, nil
}

// CreateModel creates a custom model
func (s *OllamaService) CreateModel(ctx context.Context, name, modelfile string) error {
	req := &api.CreateRequest{
		Model:    name,
		Template: modelfile, // Use Template field instead of Modelfile
	}

	err := s.client.Create(ctx, req, func(progress api.ProgressResponse) error {
		if progress.Status != "" {
			logger.GetLogger(ctx).Infof("Model creation status: %s", progress.Status)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create model: %w", err)
	}

	return nil
}

// GetModelInfo gets model information
func (s *OllamaService) GetModelInfo(ctx context.Context, modelName string) (*api.ShowResponse, error) {
	req := &api.ShowRequest{
		Name: modelName,
	}

	resp, err := s.client.Show(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get model information: %w", err)
	}

	return resp, nil
}

// OllamaModelInfo represents detailed information about an Ollama model
type OllamaModelInfo struct {
	Name       string    `json:"name"`
	Size       int64     `json:"size"`
	Digest     string    `json:"digest"`
	ModifiedAt time.Time `json:"modified_at"`
}

// ListModels lists all available models with basic info (names only)
func (s *OllamaService) ListModels(ctx context.Context) ([]string, error) {
	listResp, err := s.client.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get model list: %w", err)
	}

	modelNames := make([]string, len(listResp.Models))
	for i, model := range listResp.Models {
		modelNames[i] = model.Name
	}

	return modelNames, nil
}

// ListModelsDetailed lists all available models with detailed information
func (s *OllamaService) ListModelsDetailed(ctx context.Context) ([]OllamaModelInfo, error) {
	listResp, err := s.client.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get model list: %w", err)
	}
	jsonData, err := json.Marshal(listResp.Models)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal model list: %w", err)
	}
	logger.GetLogger(ctx).Infof("List models detailed: %s", string(jsonData))

	models := make([]OllamaModelInfo, len(listResp.Models))
	for i, model := range listResp.Models {
		models[i] = OllamaModelInfo{
			Name:       model.Name,
			Size:       model.Size,
			Digest:     model.Digest,
			ModifiedAt: model.ModifiedAt,
		}
	}

	return models, nil
}

// DeleteModel deletes a model
func (s *OllamaService) DeleteModel(ctx context.Context, modelName string) error {
	req := &api.DeleteRequest{
		Name: modelName,
	}

	err := s.client.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to delete model: %w", err)
	}

	return nil
}

// IsValidModelName checks if model name is valid
func IsValidModelName(name string) bool {
	// Simple check for model name format
	return name != "" && !strings.Contains(name, " ")
}

// Chat uses Ollama chat
func (s *OllamaService) Chat(ctx context.Context, req *api.ChatRequest, fn api.ChatResponseFunc) error {
	if err := s.checkAvailability(ctx); err != nil {
		return err
	}

	// Use official client Chat method
	return s.client.Chat(ctx, req, fn)
}

// Embeddings gets text embedding vectors
func (s *OllamaService) Embeddings(ctx context.Context, req *api.EmbedRequest) (*api.EmbedResponse, error) {
	if err := s.checkAvailability(ctx); err != nil {
		return nil, err
	}
	// Use official client Embed method
	return s.client.Embed(ctx, req)
}

// Generate generates text (used for Rerank)
func (s *OllamaService) Generate(ctx context.Context, req *api.GenerateRequest, fn api.GenerateResponseFunc) error {
	if err := s.checkAvailability(ctx); err != nil {
		return err
	}

	// Use official client Generate method
	return s.client.Generate(ctx, req, fn)
}

// GetClient returns the underlying ollama client for advanced operations
func (s *OllamaService) GetClient() *api.Client {
	return s.client
}
