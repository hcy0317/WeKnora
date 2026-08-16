package types

import "time"

type QuestionGenerationManifestState string

const (
	QuestionGenerationManifestPrepared     QuestionGenerationManifestState = "prepared"
	QuestionGenerationManifestIndexing     QuestionGenerationManifestState = "indexing"
	QuestionGenerationManifestIndexed      QuestionGenerationManifestState = "indexed"
	QuestionGenerationManifestPublished    QuestionGenerationManifestState = "published"
	QuestionGenerationManifestAbortCleanup QuestionGenerationManifestState = "abort_cleanup"
)

// QuestionGenerationManifest durably records one canonical question publication.
// A unique logical key makes concurrent workers converge on the first persisted
// question set. Questions and source IDs are JSON arrays encoded in generation
// order so retries can resume without another LLM call.
type QuestionGenerationManifest struct {
	ID                 string                          `gorm:"type:varchar(36);primaryKey" json:"id"`
	TenantID           uint64                          `gorm:"not null;uniqueIndex:uq_question_generation_manifest,priority:1" json:"tenant_id"`
	KnowledgeID        string                          `gorm:"type:varchar(36);not null;uniqueIndex:uq_question_generation_manifest,priority:2;index" json:"knowledge_id"`
	KnowledgeBaseID    string                          `gorm:"type:varchar(36);not null" json:"knowledge_base_id"`
	ChunkID            string                          `gorm:"type:varchar(36);not null;uniqueIndex:uq_question_generation_manifest,priority:3;index" json:"chunk_id"`
	ContentRevision    int                             `gorm:"not null;uniqueIndex:uq_question_generation_manifest,priority:4" json:"content_revision"`
	BatchIndex         int                             `gorm:"not null;uniqueIndex:uq_question_generation_manifest,priority:5" json:"batch_index"`
	Attempt            int                             `gorm:"not null;default:0" json:"attempt"`
	IdentityVersion    int                             `gorm:"not null" json:"identity_version"`
	GenerationKey      string                          `gorm:"type:varchar(255);not null;index" json:"generation_key"`
	TaskID             string                          `gorm:"type:varchar(255);not null" json:"task_id"`
	VectorStoreID      string                          `gorm:"type:varchar(36);not null;default:''" json:"vector_store_id"`
	EmbeddingModelID   string                          `gorm:"type:varchar(36);not null" json:"embedding_model_id"`
	EmbeddingDimension int                             `gorm:"not null" json:"embedding_dimension"`
	KnowledgeType      string                          `gorm:"type:varchar(32);not null" json:"knowledge_type"`
	EffectiveEngines   JSON                            `gorm:"type:json;not null" json:"effective_engines"`
	State              QuestionGenerationManifestState `gorm:"type:varchar(32);not null" json:"state"`
	Questions          JSON                            `gorm:"type:json;not null" json:"questions"`
	IndexEntries       JSON                            `gorm:"type:json;not null" json:"index_entries"`
	DesiredSourceIDs   JSON                            `gorm:"type:json;not null" json:"desired_source_ids"`
	AbandonedSourceIDs JSON                            `gorm:"type:json;not null" json:"abandoned_source_ids"`
	CreatedAt          time.Time                       `json:"created_at"`
	UpdatedAt          time.Time                       `json:"updated_at"`
}

type QuestionGenerationManifestKey struct {
	TenantID        uint64
	KnowledgeID     string
	ChunkID         string
	ContentRevision int
	BatchIndex      int
}

func (m *QuestionGenerationManifest) Key() QuestionGenerationManifestKey {
	if m == nil {
		return QuestionGenerationManifestKey{}
	}
	return QuestionGenerationManifestKey{
		TenantID: m.TenantID, KnowledgeID: m.KnowledgeID, ChunkID: m.ChunkID,
		ContentRevision: m.ContentRevision, BatchIndex: m.BatchIndex,
	}
}
