package routing

import (
	"net/url"
	"os"
	"strings"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
)

const GatewayBaseURLEnv = "WEKNORA_ENGINE_GATEWAY_URL"

var managedDirectEndpoints = map[lifecycle.Group]map[string]struct{}{
	lifecycle.GroupPaddleOCR: {
		"http://paddleocr-vl:8080": {},
	},
	lifecycle.GroupASR: {
		"http://speaches:8000/v1": {},
	},
	lifecycle.GroupReranker: {
		"http://accelerator-router:18083/v1": {},
		"http://qwen-reranker-gpu:8000":      {},
		"http://qwen-reranker-gpu:8000/v1":   {},
		"http://qwen-reranker:8000":          {},
		"http://qwen-reranker:8000/v1":       {},
	},
}

// Resolve redirects only the fixed, locally managed engine endpoints through
// the always-on gateway. Arbitrary internal and external model URLs are left
// untouched. Removing the environment variable is the direct-routing rollback.
func Resolve(group lifecycle.Group, endpoint string) (string, bool) {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	gatewayBaseURL, ok := gatewayBaseURL()
	if !ok {
		return endpoint, false
	}
	target, ok := gatewayTarget(gatewayBaseURL, group)
	if !ok {
		return endpoint, false
	}
	if endpoint == target {
		return target, true
	}
	if _, managed := managedDirectEndpoints[group][endpoint]; !managed {
		return endpoint, false
	}
	return target, true
}

func gatewayBaseURL() (string, bool) {
	raw := strings.TrimRight(strings.TrimSpace(os.Getenv(GatewayBaseURLEnv)), "/")
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.Hostname() != "engine-gateway" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return "", false
	}
	return raw, true
}

func gatewayTarget(baseURL string, group lifecycle.Group) (string, bool) {
	switch group {
	case lifecycle.GroupPaddleOCR:
		return baseURL + "/paddleocr", true
	case lifecycle.GroupASR:
		return baseURL + "/asr/v1", true
	case lifecycle.GroupReranker:
		return baseURL + "/reranker", true
	default:
		return "", false
	}
}
