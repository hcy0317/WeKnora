package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/vlm"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingImageVLM struct{}

func (failingImageVLM) Predict(context.Context, [][]byte, string) (string, error) {
	return "", errors.New("invalid image")
}
func (failingImageVLM) GetModelName() string { return "vision-test" }
func (failingImageVLM) GetModelID() string   { return "vlm-1" }

type failingImageModelService struct {
	interfaces.ModelService
}

func (failingImageModelService) GetVLMModel(context.Context, string) (vlm.VLM, error) {
	return failingImageVLM{}, nil
}

type imageOutcomeTracker struct {
	SpanTracker
	imageSpan  *Span
	failed     *Span
	failedCode string
	ended      *Span
}

func (t *imageOutcomeTracker) LookupStage(
	context.Context, string, int, string,
) *Span {
	return &Span{
		KnowledgeID: "kid-image-outcome",
		Attempt:     1,
		SpanID:      "multimodal-stage",
		Name:        types.StageMultimodal,
		Kind:        types.SpanKindStage,
		Status:      types.SpanStatusRunning,
	}
}

func (t *imageOutcomeTracker) BeginSubSpan(
	_ context.Context, parent *Span, name, kind string, _ types.JSONMap,
) *Span {
	t.imageSpan = &Span{
		KnowledgeID:  parent.KnowledgeID,
		Attempt:      parent.Attempt,
		SpanID:       "image-2",
		ParentSpanID: parent.SpanID,
		Name:         name,
		Kind:         kind,
		Status:       types.SpanStatusRunning,
	}
	return t.imageSpan
}

func (t *imageOutcomeTracker) FailSpan(
	_ context.Context, span *Span, code, _ string, _ error,
) {
	t.failed = span
	t.failedCode = code
}

func (t *imageOutcomeTracker) EndSpan(_ context.Context, span *Span, _ types.JSONMap) {
	t.ended = span
}

func TestMultimodalImageOutcomeError(t *testing.T) {
	vlmErr := errors.New("invalid image")

	assert.NoError(t, multimodalImageOutcomeError(1, []error{vlmErr}),
		"a usable OCR or caption chunk is a partial success")
	assert.NoError(t, multimodalImageOutcomeError(0, nil),
		"an empty but error-free model response is not a transport failure")
	require.ErrorContains(t, multimodalImageOutcomeError(0, []error{vlmErr}), "invalid image")
}

func TestImageMultimodalHandleMarksAllVLMFailuresAsFailed(t *testing.T) {
	imagePath := t.TempDir() + string(os.PathSeparator) + "image.png"
	require.NoError(t, os.WriteFile(imagePath, validServiceTestPNG(t), 0o600))
	tracker := &imageOutcomeTracker{}
	kb := &types.KnowledgeBase{
		ID: "kb-1",
		VLMConfig: types.VLMConfig{
			Enabled: true,
			ModelID: "vlm-1",
		},
	}
	svc := &ImageMultimodalService{
		modelService:  failingImageModelService{},
		knowledgeRepo: &orphanKnowledgeRepo{knowledge: &types.Knowledge{ParseStatus: types.ParseStatusProcessing}},
		kbService:     &orphanKBService{kb: kb},
		spanTracker:   tracker,
	}
	payload, err := json.Marshal(types.ImageMultimodalPayload{
		TenantID:        1,
		KnowledgeID:     "kid-image-outcome",
		KnowledgeBaseID: kb.ID,
		ImageURL:        "https://example.invalid/image.png",
		ImageLocalPath:  imagePath,
		EnableOCR:       true,
		EnableCaption:   true,
		Attempt:         1,
		ImageIndex:      2,
	})
	require.NoError(t, err)

	require.NoError(t, svc.Handle(t.Context(), asynq.NewTask(types.TypeImageMultimodal, payload)),
		"the queue item is consumed so sibling images and postprocess can continue")
	require.NotNil(t, tracker.failed)
	assert.Equal(t, "multimodal.image[2]", tracker.failed.Name)
	assert.Equal(t, "MULTIMODAL_VLM_FAILED", tracker.failedCode)
	assert.Nil(t, tracker.ended, "an image with only VLM errors must not be marked done")
}

func TestImageMultimodalHandleMarksUnreadableImageAsFailed(t *testing.T) {
	tracker := &imageOutcomeTracker{}
	kb := &types.KnowledgeBase{
		ID: "kb-1",
		VLMConfig: types.VLMConfig{
			Enabled: true,
			ModelID: "vlm-1",
		},
	}
	svc := &ImageMultimodalService{
		modelService:  failingImageModelService{},
		knowledgeRepo: &orphanKnowledgeRepo{knowledge: &types.Knowledge{ParseStatus: types.ParseStatusProcessing}},
		kbService:     &orphanKBService{kb: kb},
		spanTracker:   tracker,
	}
	payload, err := json.Marshal(types.ImageMultimodalPayload{
		TenantID:        1,
		KnowledgeID:     "kid-image-outcome",
		KnowledgeBaseID: kb.ID,
		ImageURL:        "not-a-url",
		ImageLocalPath:  t.TempDir() + string(os.PathSeparator) + "missing.png",
		EnableOCR:       true,
		EnableCaption:   true,
		Attempt:         1,
		ImageIndex:      2,
	})
	require.NoError(t, err)

	require.NoError(t, svc.Handle(t.Context(), asynq.NewTask(types.TypeImageMultimodal, payload)))
	require.NotNil(t, tracker.failed)
	assert.Equal(t, "multimodal.image[2]", tracker.failed.Name)
	assert.Equal(t, "MULTIMODAL_IMAGE_READ_FAILED", tracker.failedCode)
	assert.Nil(t, tracker.ended, "an unreadable image must not be marked done")
}

func TestSpanTracker_RecordGeneration_AttachesUnderMultimodalImage(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := t.Context()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-multimodal-usage", "trace-root")
	require.NoError(t, err)
	stage := tracker.BeginStage(ctx, "kid-multimodal-usage", attempt, types.StageMultimodal, nil)
	require.NotNil(t, stage)
	imageSpan := tracker.BeginSubSpan(
		ctx, stage, "multimodal.image[2]", types.SpanKindGeneration, nil,
	)
	require.NotNil(t, imageSpan)

	tracker.RecordGeneration(ctx, types.KnowledgeGenerationUsage{
		KnowledgeID: "kid-multimodal-usage",
		Attempt:     attempt,
		SpanID:      "vlm-generation-2",
		Stage:       imageSpan.Name,
		TaskType:    types.TypeImageMultimodal,
		Name:        "vlm.predict",
		ModelType:   "vlm",
		ModelName:   "vision-test",
		Status:      types.SpanStatusDone,
	})

	var row types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND span_id = ?", "kid-multimodal-usage", "vlm-generation-2").
		First(&row).Error)
	assert.Equal(t, imageSpan.SpanID, row.ParentSpanID)

	// Prove the parent lookup is exact rather than falling back to the broad
	// multimodal stage, which was the original UI hierarchy bug.
	repo := repository.NewKnowledgeSpanRepository(db)
	rows, err := repo.ListByAttempt(ctx, "kid-multimodal-usage", attempt)
	require.NoError(t, err)
	require.Len(t, rows, 4)
}

func validServiceTestPNG(t *testing.T) []byte {
	t.Helper()
	const encoded = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
	data, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)
	return data
}
