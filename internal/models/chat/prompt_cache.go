package chat

import (
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/models/provider"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/sashabaranov/go-openai"
)

func providerCacheAccountingStatus(name provider.ProviderName) types.PromptCacheStatus {
	switch name {
	case provider.ProviderOpenAI,
		provider.ProviderAzureOpenAI,
		provider.ProviderDeepSeek,
		provider.ProviderAliyun,
		provider.ProviderAnthropic:
		return types.PromptCacheStatusUnreported
	default:
		return types.PromptCacheStatusUnsupported
	}
}

func tokenUsageFromOpenAI(usage openai.Usage, providerName provider.ProviderName) types.TokenUsage {
	u := types.TokenUsage{
		PromptTokens:     usage.PromptTokens,
		CompletionTokens: usage.CompletionTokens,
		TotalTokens:      usage.TotalTokens,
	}
	if usage.PromptTokensDetails != nil {
		read := usage.PromptTokensDetails.CachedTokens
		u.SetPromptCacheUsage(read, 0, max(0, usage.PromptTokens-read), true)
		return u
	}
	if providerCacheAccountingStatus(providerName) == types.PromptCacheStatusUnsupported {
		u.MarkPromptCacheUnsupported()
	} else {
		u.SetPromptCacheUsage(0, 0, 0, false)
	}
	return u
}

// cachedTokens is retained as the nil-safe primitive used by older callers
// and focused tests; normalization happens in tokenUsageFromOpenAI.
func cachedTokens(details *openai.PromptTokensDetails) int {
	if details == nil {
		return 0
	}
	return details.CachedTokens
}

type rawPromptCacheUsage struct {
	Usage struct {
		PromptTokens        int  `json:"prompt_tokens"`
		PromptCacheHit      *int `json:"prompt_cache_hit_tokens"`
		PromptCacheMiss     *int `json:"prompt_cache_miss_tokens"`
		CacheReadInput      *int `json:"cache_read_input_tokens"`
		CacheCreationInput  *int `json:"cache_creation_input_tokens"`
		PromptTokensDetails *struct {
			CachedTokens     *int `json:"cached_tokens"`
			CacheWriteTokens *int `json:"cache_write_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}

// applyRawPromptCacheUsage captures native fields discarded by the pinned
// OpenAI-compatible SDK (notably DeepSeek hit/miss counters).
func applyRawPromptCacheUsage(data []byte, usage *types.TokenUsage) {
	if usage == nil || len(data) == 0 {
		return
	}
	var raw rawPromptCacheUsage
	if json.Unmarshal(data, &raw) != nil {
		return
	}
	if raw.Usage.PromptCacheHit != nil || raw.Usage.PromptCacheMiss != nil {
		read := valueOrZero(raw.Usage.PromptCacheHit)
		miss := valueOrZero(raw.Usage.PromptCacheMiss)
		usage.SetPromptCacheUsage(read, 0, miss, true)
		return
	}
	if raw.Usage.CacheReadInput != nil || raw.Usage.CacheCreationInput != nil {
		read := valueOrZero(raw.Usage.CacheReadInput)
		write := valueOrZero(raw.Usage.CacheCreationInput)
		usage.SetPromptCacheUsage(read, write, max(0, usage.PromptTokens-read), true)
		return
	}
	if details := raw.Usage.PromptTokensDetails; details != nil {
		read := valueOrZero(details.CachedTokens)
		write := valueOrZero(details.CacheWriteTokens)
		usage.SetPromptCacheUsage(read, write, max(0, usage.PromptTokens-read), true)
	}
}

func valueOrZero(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}
