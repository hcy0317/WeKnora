package handler

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
)

type tokenUsageMetrics struct {
	Calls            int   `json:"calls"`
	MeasuredCalls    int   `json:"measured_calls"`
	EstimatedCalls   int   `json:"estimated_calls"`
	UnknownCalls     int   `json:"unknown_calls"`
	InputTokens      int64 `json:"input_tokens"`
	OutputTokens     int64 `json:"output_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	CacheReadTokens  int64 `json:"cache_read_tokens"`
	CacheWriteTokens int64 `json:"cache_write_tokens"`
	CacheMissTokens  int64 `json:"cache_miss_tokens"`
}

type tokenUsageModelSummary struct {
	ModelType string            `json:"model_type"`
	ModelID   string            `json:"model_id,omitempty"`
	ModelName string            `json:"model_name"`
	Usage     tokenUsageMetrics `json:"usage"`
}

type tokenUsageStageSummary struct {
	Stage  string                   `json:"stage"`
	Usage  tokenUsageMetrics        `json:"usage"`
	Models []tokenUsageModelSummary `json:"models"`
}

type knowledgeTokenUsageSummary struct {
	HasData bool                     `json:"has_data"`
	Usage   tokenUsageMetrics        `json:"usage"`
	Stages  []tokenUsageStageSummary `json:"stages"`
}

type mutableTokenStage struct {
	usage  tokenUsageMetrics
	models map[string]*tokenUsageModelSummary
}

func summarizeKnowledgeTokenUsage(rows []types.KnowledgeProcessingSpan) knowledgeTokenUsageSummary {
	result := knowledgeTokenUsageSummary{Stages: []tokenUsageStageSummary{}}
	stages := make(map[string]*mutableTokenStage)
	for _, row := range rows {
		if row.Kind != types.SpanKindGeneration {
			continue
		}
		usage := jsonMapValue(row.Output, "usage")
		if len(usage) == 0 {
			usage = jsonMapValue(row.Metadata, "usage")
		}
		unit := strings.ToUpper(stringValue(usage["unit"]))
		if unit != "" && unit != "TOKENS" {
			continue
		}
		stageName := stringValue(row.Metadata["processing_stage"])
		if stageName == "" {
			stageName = "unattributed"
		}
		stage := stages[stageName]
		if stage == nil {
			stage = &mutableTokenStage{models: make(map[string]*tokenUsageModelSummary)}
			stages[stageName] = stage
		}
		modelType := stringValue(row.Metadata["model_type"])
		modelID := stringValue(row.Metadata["model_id"])
		modelName := stringValue(row.Metadata["model_name"])
		if modelName == "" {
			modelName = "unknown"
		}
		modelKey := modelType + "\x00" + modelID + "\x00" + modelName
		model := stage.models[modelKey]
		if model == nil {
			model = &tokenUsageModelSummary{ModelType: modelType, ModelID: modelID, ModelName: modelName}
			stage.models[modelKey] = model
		}
		metrics := usageMetricsFromMap(usage)
		addTokenUsage(&result.Usage, metrics)
		addTokenUsage(&stage.usage, metrics)
		addTokenUsage(&model.Usage, metrics)
	}

	stageNames := make([]string, 0, len(stages))
	for stageName := range stages {
		stageNames = append(stageNames, stageName)
	}
	sort.Slice(stageNames, func(i, j int) bool {
		left, right := stageSortKey(stageNames[i]), stageSortKey(stageNames[j])
		if left != right {
			return left < right
		}
		return stageNames[i] < stageNames[j]
	})
	for _, stageName := range stageNames {
		stage := stages[stageName]
		models := make([]tokenUsageModelSummary, 0, len(stage.models))
		for _, model := range stage.models {
			models = append(models, *model)
		}
		sort.Slice(models, func(i, j int) bool {
			if models[i].ModelType != models[j].ModelType {
				return models[i].ModelType < models[j].ModelType
			}
			return models[i].ModelName < models[j].ModelName
		})
		result.Stages = append(result.Stages, tokenUsageStageSummary{
			Stage: stageName, Usage: stage.usage, Models: models,
		})
	}
	result.HasData = result.Usage.Calls > 0
	return result
}

func usageMetricsFromMap(usage types.JSONMap) tokenUsageMetrics {
	available := boolValue(usage["available"])
	estimated := boolValue(usage["estimated"])
	metrics := tokenUsageMetrics{
		Calls:            1,
		InputTokens:      int64Value(usage["input_tokens"]),
		OutputTokens:     int64Value(usage["output_tokens"]),
		TotalTokens:      int64Value(usage["total_tokens"]),
		CacheReadTokens:  int64Value(usage["cache_read_tokens"]),
		CacheWriteTokens: int64Value(usage["cache_write_tokens"]),
		CacheMissTokens:  int64Value(usage["cache_miss_tokens"]),
	}
	if available && estimated {
		metrics.EstimatedCalls = 1
	} else if available {
		metrics.MeasuredCalls = 1
	} else {
		metrics.UnknownCalls = 1
	}
	return metrics
}

func addTokenUsage(dst *tokenUsageMetrics, src tokenUsageMetrics) {
	dst.Calls += src.Calls
	dst.MeasuredCalls += src.MeasuredCalls
	dst.EstimatedCalls += src.EstimatedCalls
	dst.UnknownCalls += src.UnknownCalls
	dst.InputTokens += src.InputTokens
	dst.OutputTokens += src.OutputTokens
	dst.TotalTokens += src.TotalTokens
	dst.CacheReadTokens += src.CacheReadTokens
	dst.CacheWriteTokens += src.CacheWriteTokens
	dst.CacheMissTokens += src.CacheMissTokens
}

func stageSortKey(stage string) int {
	switch {
	case stage == types.StageDocReader || strings.HasPrefix(stage, types.StageDocReader+"."):
		return 10
	case stage == types.StageChunking || strings.HasPrefix(stage, types.StageChunking+"."):
		return 20
	case stage == types.StageEmbedding || strings.HasPrefix(stage, types.StageEmbedding+"."):
		return 30
	case stage == types.StageMultimodal || strings.HasPrefix(stage, types.StageMultimodal+"."):
		return 40
	case stage == types.StagePostProcess || strings.HasPrefix(stage, types.StagePostProcess+"."):
		return 50
	default:
		return 90
	}
}

func jsonMapValue(parent types.JSONMap, key string) types.JSONMap {
	value := parent[key]
	switch typed := value.(type) {
	case types.JSONMap:
		return typed
	case map[string]interface{}:
		return types.JSONMap(typed)
	default:
		return types.JSONMap{}
	}
}

func stringValue(value interface{}) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprint(value)
}

func int64Value(value interface{}) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case float32:
		return int64(typed)
	case float64:
		return int64(typed)
	case string:
		parsed, _ := strconv.ParseInt(typed, 10, 64)
		return parsed
	default:
		return 0
	}
}

func boolValue(value interface{}) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		parsed, _ := strconv.ParseBool(typed)
		return parsed
	default:
		return false
	}
}
