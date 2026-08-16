package types

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	reparseCleanupInputKey = "reparse_cleanup"
	indexRouteInputKey     = "index_route"
)

const (
	ReparseCleanupVersion   = 1
	ReparseCleanupPending   = "pending"
	ReparseCleanupPrepared  = "prepared"
	ReparseCleanupCompleted = "completed"
)

// ReparseCleanupCheckpoint is the durable, attempt-scoped cleanup plan stored
// on that attempt's root span. The shared knowledge row deliberately does not
// carry this state: a newer attempt must never overwrite an older attempt's
// recovery history.
type ReparseCleanupCheckpoint struct {
	Version                   int                     `json:"version"`
	Attempt                   int                     `json:"attempt"`
	Phase                     string                  `json:"phase"`
	SourceEmbeddingModelID    string                  `json:"source_embedding_model_id,omitempty"`
	TargetEmbeddingModelID    string                  `json:"target_embedding_model_id,omitempty"`
	SourceVectorStoreID       *string                 `json:"source_vector_store_id,omitempty"`
	SourceEffectiveEngines    []RetrieverEngineParams `json:"source_effective_engines,omitempty"`
	TargetVectorStoreID       *string                 `json:"target_vector_store_id,omitempty"`
	TargetEffectiveEngines    []RetrieverEngineParams `json:"target_effective_engines,omitempty"`
	KnowledgeType             string                  `json:"knowledge_type,omitempty"`
	EmbeddingDimensions       int                     `json:"embedding_dimensions,omitempty"`
	ImageURLs                 []string                `json:"image_urls,omitempty"`
	WikiCleanupRequired       bool                    `json:"wiki_cleanup_required,omitempty"`
	WikiPendingIngestScrubbed bool                    `json:"wiki_pending_ingest_scrubbed,omitempty"`
	VectorsDeleted            bool                    `json:"vectors_deleted,omitempty"`
	ChunksDeleted             bool                    `json:"chunks_deleted,omitempty"`
	ImagesDeleted             bool                    `json:"images_deleted,omitempty"`
	GraphDeleted              bool                    `json:"graph_deleted,omitempty"`
}

// KnowledgeIndexRouteSnapshot records the concrete embedding model and vector
// routing accepted by an attempt before it can write external index data. The
// next reparse copies this immutable snapshot into its source cleanup plan, so
// changing tenant defaults cannot redirect deletion to a different backend or
// use incompatible embedding dimensions.
type KnowledgeIndexRouteSnapshot struct {
	Version          int                     `json:"version"`
	EmbeddingModelID string                  `json:"embedding_model_id,omitempty"`
	VectorStoreID    *string                 `json:"vector_store_id,omitempty"`
	EffectiveEngines []RetrieverEngineParams `json:"effective_engines,omitempty"`
}

func PutKnowledgeIndexRouteSnapshot(
	rootInput JSONMap, snapshot KnowledgeIndexRouteSnapshot,
) (JSONMap, error) {
	if snapshot.Version != 1 {
		return nil, fmt.Errorf("knowledge index route snapshot: unsupported version %d", snapshot.Version)
	}
	if snapshot.VectorStoreID == nil && len(snapshot.EffectiveEngines) == 0 {
		return nil, errors.New("knowledge index route snapshot: vector store or effective engines are required")
	}
	updated := make(JSONMap, len(rootInput)+1)
	for key, value := range rootInput {
		updated[key] = value
	}
	updated[indexRouteInputKey] = snapshot
	return updated, nil
}

func DecodeKnowledgeIndexRouteSnapshot(rootInput JSONMap) (*KnowledgeIndexRouteSnapshot, error) {
	if rootInput == nil {
		return nil, nil
	}
	raw, ok := rootInput[indexRouteInputKey]
	if !ok {
		return nil, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("encode knowledge index route snapshot: %w", err)
	}
	var snapshot KnowledgeIndexRouteSnapshot
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		return nil, fmt.Errorf("decode knowledge index route snapshot: %w", err)
	}
	if _, err := PutKnowledgeIndexRouteSnapshot(nil, snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func validateReparseCleanupCheckpoint(checkpoint ReparseCleanupCheckpoint) error {
	if checkpoint.Version != ReparseCleanupVersion {
		return fmt.Errorf("reparse cleanup checkpoint: unsupported version %d", checkpoint.Version)
	}
	if checkpoint.Attempt <= 0 {
		return errors.New("reparse cleanup checkpoint: positive attempt is required")
	}
	switch checkpoint.Phase {
	case ReparseCleanupPending:
	case ReparseCleanupPrepared, ReparseCleanupCompleted:
		if checkpoint.SourceEmbeddingModelID != "" && checkpoint.EmbeddingDimensions <= 0 {
			return errors.New("reparse cleanup checkpoint: embedding dimensions are required for prepared vector cleanup")
		}
	default:
		return errors.New("reparse cleanup checkpoint: invalid phase")
	}
	return nil
}

// PutReparseCleanupCheckpoint returns a copy of rootInput containing the
// checkpoint. It never mutates the caller's map, which prevents a failed span
// write from making in-memory state look durable.
func PutReparseCleanupCheckpoint(rootInput JSONMap, checkpoint ReparseCleanupCheckpoint) (JSONMap, error) {
	if err := validateReparseCleanupCheckpoint(checkpoint); err != nil {
		return nil, err
	}
	updated := make(JSONMap, len(rootInput)+1)
	for key, value := range rootInput {
		updated[key] = value
	}
	updated[reparseCleanupInputKey] = checkpoint
	return updated, nil
}

func DecodeReparseCleanupCheckpoint(rootInput JSONMap) (*ReparseCleanupCheckpoint, error) {
	if rootInput == nil {
		return nil, nil
	}
	raw, ok := rootInput[reparseCleanupInputKey]
	if !ok {
		return nil, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("encode reparse cleanup checkpoint: %w", err)
	}
	var checkpoint ReparseCleanupCheckpoint
	if err := json.Unmarshal(encoded, &checkpoint); err != nil {
		return nil, fmt.Errorf("decode reparse cleanup checkpoint: %w", err)
	}
	if err := validateReparseCleanupCheckpoint(checkpoint); err != nil {
		return nil, err
	}
	return &checkpoint, nil
}
