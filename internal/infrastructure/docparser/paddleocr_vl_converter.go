package docparser

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
)

const (
	paddleOCRVLTimeout              = 1000 * time.Second // large scanned PDFs can take a while
	paddleOCRVLMaxErrorSummaryRunes = 1024
	paddleOCRVLMaxErrorInputBytes   = paddleOCRVLMaxErrorSummaryRunes * 4
	paddleOCRVLMaxErrorBodyBytes    = 64 * 1024
	paddleOCRVLFileOverlapWindow    = 8
)

var paddleOCRVLSecretPattern = regexp.MustCompile(
	`(?i)(^|[\s"'?&;,{}()])([a-z0-9_-]*(?:api[-_]?key|access[-_]?token|authorization|password|secret|token))["']?\s*[:=]\s*(?:"[^"]*"|'[^']*'|(?:basic|bearer)\s+[^\s,;}&]+|[^\s,;}&]+)`,
)

var paddleOCRVLStandaloneAuthPattern = regexp.MustCompile(
	`(?i)\b(?:basic|bearer)\s+[^\s,;}&"'<>]+`,
)

var paddleOCRVLBase64CandidatePattern = regexp.MustCompile(
	`[A-Za-z0-9+/_-]{8,}={0,2}`,
)

// PaddleOCRVLReader calls a self-hosted PaddleOCR-VL pipeline service
// (the full document-parsing API, not the bare VLM inference server).
//
// Flow: POST {endpoint}/layout-parsing with base64 file → synchronous JSON
// response containing per-page markdown + inline base64 images.
type PaddleOCRVLReader struct {
	endpoint string
	useSeal  bool
	useChart bool
}

// NewPaddleOCRVLReader creates a reader from ParserEngineOverrides.
func NewPaddleOCRVLReader(overrides map[string]string) *PaddleOCRVLReader {
	return &PaddleOCRVLReader{
		endpoint: strings.TrimRight(overrides["paddleocr_vl_endpoint"], "/"),
		useSeal:  parseBoolOr(overrides["paddleocr_vl_use_seal_recognition"], true),
		useChart: parseBoolOr(overrides["paddleocr_vl_use_chart_recognition"], false),
	}
}

func (c *PaddleOCRVLReader) Read(ctx context.Context, req *types.ReadRequest) (*types.ReadResult, error) {
	if c.endpoint == "" {
		return &types.ReadResult{Error: "PaddleOCR-VL endpoint is not configured"}, nil
	}
	if _, err := paddleOCRVLParsedEndpoint(c.endpoint); err != nil {
		return nil, fmt.Errorf("PaddleOCR-VL endpoint=%q: %w", "<invalid endpoint>", err)
	}
	if err := utils.ValidateURLForSSRF(c.endpoint); err != nil {
		endpoint := paddleOCRVLDiagnosticEndpoint(c.endpoint)
		logger.Errorf(ctx, "[PaddleOCR-VL] endpoint=%q blocked by SSRF policy", endpoint)
		return nil, fmt.Errorf("PaddleOCR-VL endpoint=%q is not allowed by SSRF policy", endpoint)
	}

	content := req.FileContent
	if len(content) == 0 {
		return &types.ReadResult{Error: "no file content provided"}, nil
	}

	requestID := utils.SanitizeForLog(req.RequestID)
	if requestID != "" {
		ctx = logger.WithRequestID(ctx, requestID)
	}
	fileName := utils.SanitizeForLog(req.FileName)
	endpoint := paddleOCRVLDiagnosticEndpoint(c.endpoint)
	logger.Infof(ctx, "[PaddleOCR-VL] Parsing file=%s size=%d via %s",
		fileName, len(content), endpoint)

	mdContent, imagesB64, err := c.callLayoutParsing(ctx, req, content)
	if err != nil {
		logger.Errorf(ctx, "[PaddleOCR-VL] layout-parsing failed endpoint=%q file=%q requestID=%q: %v",
			endpoint, fileName, requestID, err)
		return nil, fmt.Errorf(
			"PaddleOCR-VL layout-parsing endpoint=%q file=%q request_id=%q: %w",
			endpoint, fileName, requestID, err,
		)
	}

	// PaddleOCR-VL renders tables as styled HTML (per-cell text-align), which
	// wastes tokens and defeats the chunker's table-protection logic. Convert
	// them to Markdown tables (or strip layout attributes when conversion is
	// not possible) before downstream processing.
	mdContent = normalizeHTMLTables(mdContent)

	imageRefs, mdContent := c.processImages(ctx, mdContent, imagesB64)
	mdContent, imageRefs = ensureOriginalImageRef(req, mdContent, imageRefs)

	logger.Infof(ctx, "[PaddleOCR-VL] Parsed successfully, markdown=%d chars, images=%d",
		len(mdContent), len(imageRefs))

	return &types.ReadResult{
		MarkdownContent: mdContent,
		ImageRefs:       imageRefs,
	}, nil
}

func paddleOCRVLDiagnosticEndpoint(endpoint string) string {
	u, err := paddleOCRVLParsedEndpoint(endpoint)
	if err != nil {
		return "<invalid endpoint>"
	}
	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""
	return utils.SanitizeForLog(u.String())
}

func paddleOCRVLParsedEndpoint(endpoint string) (*url.URL, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Opaque != "" || u.Host == "" {
		return nil, errors.New("invalid configured endpoint")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, errors.New("invalid configured endpoint")
	}
	return u, nil
}

func paddleOCRVLRequestURL(endpoint, suffix string) (string, error) {
	u, err := paddleOCRVLParsedEndpoint(endpoint)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + suffix
	u.RawPath = ""
	return u.String(), nil
}

func paddleOCRVLHashWindow(value []byte) uint64 {
	const base uint64 = 257
	var hash uint64
	for _, current := range value {
		hash = hash*base + uint64(current)
	}
	return hash
}

func paddleOCRVLHashPower(window int) uint64 {
	const base uint64 = 257
	power := uint64(1)
	for index := 1; index < window; index++ {
		power *= base
	}
	return power
}

func paddleOCRVLAddWindowCandidates(
	value []byte,
	window int,
	candidates map[uint64][][]byte,
) {
	if len(value) < window {
		return
	}
	const base uint64 = 257
	power := paddleOCRVLHashPower(window)
	hash := paddleOCRVLHashWindow(value[:window])
	for offset := 0; ; offset++ {
		candidates[hash] = append(candidates[hash], value[offset:offset+window])
		if offset+window >= len(value) {
			break
		}
		hash -= uint64(value[offset]) * power
		hash = hash*base + uint64(value[offset+window])
	}
}

func paddleOCRVLContainsCandidateWindow(
	value []byte,
	window int,
	candidates map[uint64][][]byte,
) bool {
	if len(value) < window || len(candidates) == 0 {
		return false
	}
	const base uint64 = 257
	power := paddleOCRVLHashPower(window)
	hash := paddleOCRVLHashWindow(value[:window])
	for offset := 0; ; offset++ {
		for _, candidate := range candidates[hash] {
			if bytes.Equal(value[offset:offset+window], candidate) {
				return true
			}
		}
		if offset+window >= len(value) {
			break
		}
		hash -= uint64(value[offset]) * power
		hash = hash*base + uint64(value[offset+window])
	}
	return false
}

func paddleOCRVLSharedWindow(message, fileContent []byte) bool {
	window := paddleOCRVLFileOverlapWindow
	if len(message) == 0 || len(fileContent) == 0 {
		return false
	}
	if len(message) < window || len(fileContent) < window {
		return bytes.Contains(message, fileContent) || bytes.Contains(fileContent, message)
	}

	candidates := make(map[uint64][][]byte, len(message)-window+1)
	paddleOCRVLAddWindowCandidates(message, window, candidates)
	return paddleOCRVLContainsCandidateWindow(fileContent, window, candidates)
}

func paddleOCRVLContainsEncodedFileContent(message, fileContent []byte) bool {
	if len(message) == 0 || len(fileContent) == 0 {
		return false
	}
	encodings := []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	}
	window := paddleOCRVLFileOverlapWindow
	windowCandidates := make(map[uint64][][]byte)
	for _, candidate := range paddleOCRVLBase64CandidatePattern.FindAll(message, -1) {
		var decoded []byte
		for _, encoding := range encodings {
			buffer := make([]byte, encoding.DecodedLen(len(candidate)))
			decodedLength, err := encoding.Decode(buffer, candidate)
			if err != nil || decodedLength == 0 {
				continue
			}
			decoded = buffer[:decodedLength]
			break
		}
		if len(decoded) == 0 {
			continue
		}
		if len(fileContent) < window {
			if bytes.Contains(decoded, fileContent) {
				return true
			}
			continue
		}
		if len(decoded) < window {
			continue
		}
		paddleOCRVLAddWindowCandidates(decoded, window, windowCandidates)
	}
	return paddleOCRVLContainsCandidateWindow(fileContent, window, windowCandidates)
}

func paddleOCRVLErrorSummary(message string, fileContent []byte) string {
	truncated := len(message) > paddleOCRVLMaxErrorInputBytes
	if truncated {
		message = message[:paddleOCRVLMaxErrorInputBytes]
	}
	rawCandidate := []byte(message)
	if paddleOCRVLSharedWindow(rawCandidate, fileContent) ||
		paddleOCRVLContainsEncodedFileContent(rawCandidate, fileContent) {
		return "[redacted file content]"
	}

	summary := strings.TrimSpace(utils.SanitizeForLog(strings.ToValidUTF8(message, "�")))
	if summary == "" {
		return ""
	}

	runes := []rune(summary)
	if len(runes) > paddleOCRVLMaxErrorSummaryRunes {
		summary = string(runes[:paddleOCRVLMaxErrorSummaryRunes])
		truncated = true
	}
	if truncated {
		summary += "…"
	}

	// The upload remains in its original byte slice. A rolling hash finds raw
	// overlap in one pass. Base64 candidates are extracted only from the bounded
	// diagnostic, decoded, and matched without encoding or copying the document.
	candidate := []byte(strings.TrimSuffix(summary, "…"))
	if paddleOCRVLSharedWindow(candidate, fileContent) ||
		paddleOCRVLContainsEncodedFileContent(candidate, fileContent) {
		return "[redacted file content]"
	}

	summary = paddleOCRVLStandaloneAuthPattern.ReplaceAllString(summary, "***")
	return paddleOCRVLSecretPattern.ReplaceAllString(summary, "${1}${2}=***")
}

func paddleOCRVLTransportError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return fmt.Errorf("HTTP request failed: %w", urlErr.Err)
	}
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("HTTP request canceled: %w", context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("HTTP request timed out: %w", context.DeadlineExceeded)
	}
	return errors.New("HTTP request failed")
}

// paddleOCRVLRecognitionParams returns the recognition / page-restructuring
// parameters shared by the self-hosted (/layout-parsing, top-level body) and
// cloud (optionalPayload) request bodies. Keeping both identical ensures the
// self-hosted engine reproduces the cloud output: cross-page table merging,
// multi-level heading reconstruction, header/footer stripping, and the same
// sampling / resolution settings used by the AI Studio service.
func paddleOCRVLRecognitionParams(useSeal, useChart bool) map[string]interface{} {
	return map[string]interface{}{
		"markdownIgnoreLabels": []string{
			"header", "header_image", "footer", "footer_image",
			"number", "footnote", "aside_text",
		},
		"useDocOrientationClassify": false,
		"useDocUnwarping":           false,
		"useLayoutDetection":        true,
		"useChartRecognition":       useChart,
		"useSealRecognition":        useSeal,
		"useOcrForImageBlock":       false,
		"mergeTables":               true,
		"relevelTitles":             true,
		"restructurePages":          true,
		"layoutShapeMode":           "auto",
		"promptLabel":               "ocr",
		"layoutNms":                 true,
		"repetitionPenalty":         1,
		"temperature":               0,
		"topP":                      1,
		"minPixels":                 147384,
		"maxPixels":                 2822400,
	}
}

// fileTypeCode maps a request to the PaddleOCR-VL fileType field:
// 0 = PDF, 1 = image (including TIFF).
func fileTypeCode(req *types.ReadRequest) int {
	ft := strings.ToLower(strings.TrimPrefix(req.FileType, "."))
	if ft == "" {
		ft = strings.TrimPrefix(strings.ToLower(filepath.Ext(req.FileName)), ".")
	}
	if ft == "pdf" {
		return 0
	}
	return 1
}

// paddleOCRVLResponse mirrors the relevant fields of the PaddleX serving
// /layout-parsing response. The service returns one entry per page.
type paddleOCRVLResponse struct {
	ErrorCode int    `json:"errorCode"`
	ErrorMsg  string `json:"errorMsg"`
	Result    struct {
		LayoutParsingResults []struct {
			Markdown struct {
				Text   string            `json:"text"`
				Images map[string]string `json:"images"`
			} `json:"markdown"`
		} `json:"layoutParsingResults"`
	} `json:"result"`
}

func (c *PaddleOCRVLReader) callLayoutParsing(
	ctx context.Context, req *types.ReadRequest, content []byte,
) (string, map[string]string, error) {
	payload := paddleOCRVLRecognitionParams(c.useSeal, c.useChart)
	payload["file"] = base64.StdEncoding.EncodeToString(content)
	payload["fileType"] = fileTypeCode(req)
	payload["visualize"] = false

	body, err := json.Marshal(payload)
	if err != nil {
		return "", nil, fmt.Errorf("marshal payload: %w", err)
	}

	layoutURL, err := paddleOCRVLRequestURL(c.endpoint, "/layout-parsing")
	if err != nil {
		return "", nil, errors.New("create PaddleOCR-VL request: invalid configured endpoint")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, layoutURL, bytes.NewReader(body))
	if err != nil {
		return "", nil, errors.New("create PaddleOCR-VL request")
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := utils.NewSSRFSafeHTTPClient(utils.SSRFSafeHTTPClientConfig{
		Timeout:      paddleOCRVLTimeout,
		MaxRedirects: 5,
	})
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", nil, paddleOCRVLTransportError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, paddleOCRVLMaxErrorBodyBytes))
		if readErr != nil {
			return "", nil, fmt.Errorf("read error response body: %w", readErr)
		}
		summary := paddleOCRVLErrorSummary(string(respBody), content)
		if summary != "" {
			return "", nil, fmt.Errorf("PaddleOCR-VL API status %d: %s", resp.StatusCode, summary)
		}
		return "", nil, fmt.Errorf("PaddleOCR-VL API status %d", resp.StatusCode)
	}

	// Successful responses contain the complete multi-page Markdown and inline
	// images, so only non-200 diagnostics are bounded above.
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, fmt.Errorf("read response body: %w", err)
	}

	var result paddleOCRVLResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", nil, fmt.Errorf("decode response: %w", err)
	}
	if result.ErrorCode != 0 {
		summary := paddleOCRVLErrorSummary(result.ErrorMsg, content)
		if summary != "" {
			return "", nil, fmt.Errorf("PaddleOCR-VL error %d: %s", result.ErrorCode, summary)
		}
		return "", nil, fmt.Errorf("PaddleOCR-VL error %d", result.ErrorCode)
	}

	pages := result.Result.LayoutParsingResults
	if len(pages) == 0 {
		logger.Errorf(ctx, "[PaddleOCR-VL] response has no layoutParsingResults")
		return "", nil, fmt.Errorf("PaddleOCR-VL response has no layoutParsingResults")
	}

	// Merge per-page markdown and image dicts into one document.
	texts := make([]string, 0, len(pages))
	images := make(map[string]string)
	for _, p := range pages {
		if t := strings.TrimSpace(p.Markdown.Text); t != "" {
			texts = append(texts, p.Markdown.Text)
		}
		for path, data := range p.Markdown.Images {
			if _, ok := images[path]; !ok {
				images[path] = data
			}
		}
	}

	logger.Infof(ctx, "[PaddleOCR-VL] parsed %d page(s), images=%d", len(pages), len(images))
	return strings.Join(texts, "\n\n"), images, nil
}

// processImages decodes the inline base64 images returned by PaddleOCR-VL and
// builds ImageRef entries, matching them against references in the markdown.
func (c *PaddleOCRVLReader) processImages(
	ctx context.Context, mdContent string, imagesB64 map[string]string,
) ([]types.ImageRef, string) {
	var refs []types.ImageRef

	for ipath, b64Str := range imagesB64 {
		matchedRefs := mineruImageOriginalRefs(mdContent, ipath)
		if len(matchedRefs) == 0 {
			continue
		}

		var imgBytes []byte
		var ext string
		if m := b64DataURIPattern.FindStringSubmatch(b64Str); len(m) == 3 {
			ext = m[1]
			decoded, err := base64.StdEncoding.DecodeString(m[2])
			if err != nil {
				logger.Errorf(ctx, "[PaddleOCR-VL] decode base64 image %s: %v", ipath, err)
				continue
			}
			imgBytes = decoded
		} else {
			decoded, err := base64.StdEncoding.DecodeString(b64Str)
			if err != nil {
				logger.Errorf(ctx, "[PaddleOCR-VL] decode raw base64 image %s: %v", ipath, err)
				continue
			}
			imgBytes = decoded
			ext = strings.TrimPrefix(filepath.Ext(ipath), ".")
			if ext == "" {
				ext = "png"
			}
		}

		mimeType := mime.TypeByExtension("." + ext)
		if mimeType == "" {
			mimeType = "image/png"
		}

		for _, originalRef := range matchedRefs {
			refs = append(refs, types.ImageRef{
				Filename:    ipath,
				OriginalRef: originalRef,
				MimeType:    mimeType,
				ImageData:   imgBytes,
			})
		}
	}

	return refs, mdContent
}

// PingPaddleOCRVL checks whether a self-hosted PaddleOCR-VL service is reachable.
func PingPaddleOCRVL(endpoint string) (bool, string) {
	endpoint = strings.TrimRight(endpoint, "/")
	if endpoint == "" {
		return false, "未配置 PaddleOCR-VL 端点"
	}
	if err := utils.ValidateURLForSSRF(endpoint); err != nil {
		return false, fmt.Sprintf("PaddleOCR-VL 端点未通过 SSRF 校验: %v", err)
	}
	client := utils.NewSSRFSafeHTTPClient(utils.SSRFSafeHTTPClientConfig{
		Timeout:      5 * time.Second,
		MaxRedirects: 5,
	})
	return pingPaddleOCRVL(endpoint, client)
}

func pingPaddleOCRVL(endpoint string, client *http.Client) (bool, string) {
	healthURL, err := paddleOCRVLRequestURL(endpoint, "/health")
	if err != nil {
		return false, "PaddleOCR-VL 端点格式无效"
	}
	resp, err := client.Get(healthURL)
	if err != nil {
		return false, fmt.Sprintf("PaddleOCR-VL 服务不可达: %v", paddleOCRVLTransportError(err))
	}
	resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true, ""
	}
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusMethodNotAllowed {
		return false, fmt.Sprintf("PaddleOCR-VL 健康检查返回状态 %d", resp.StatusCode)
	}

	// Older pipeline versions do not expose /health. Preserve the legacy
	// reachability probe: any routed response below 500, including 405 from the
	// POST-only /layout-parsing endpoint, proves that the service is reachable.
	layoutURL, err := paddleOCRVLRequestURL(endpoint, "/layout-parsing")
	if err != nil {
		return false, "PaddleOCR-VL 端点格式无效"
	}
	resp, err = client.Get(layoutURL)
	if err != nil {
		return false, fmt.Sprintf("PaddleOCR-VL 服务不可达: %v", paddleOCRVLTransportError(err))
	}
	resp.Body.Close()
	if resp.StatusCode >= 500 {
		return false, fmt.Sprintf("PaddleOCR-VL 服务返回状态 %d", resp.StatusCode)
	}
	return true, ""
}
