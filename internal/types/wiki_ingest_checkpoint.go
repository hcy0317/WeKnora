package types

import (
	"context"
	"time"
)

type WikiIngestWorkUnitState string

const WikiIngestWorkBindingInputKey = "wiki_work_binding"

const (
	WikiIngestWorkUnitPrepared  WikiIngestWorkUnitState = "prepared"
	WikiIngestWorkUnitMapped    WikiIngestWorkUnitState = "mapped"
	WikiIngestWorkUnitAbandoned WikiIngestWorkUnitState = "abandoned"
)

type WikiSlugApplicationState string

type WikiTaxonomyPlanState string

const (
	WikiSlugApplicationPrepared  WikiSlugApplicationState = "prepared"
	WikiSlugApplicationApplying  WikiSlugApplicationState = "applying"
	WikiSlugApplicationPublished WikiSlugApplicationState = "published"
	WikiSlugApplicationAbandoned WikiSlugApplicationState = "abandoned"
)

const (
	WikiTaxonomyPlanPrepared  WikiTaxonomyPlanState = "prepared"
	WikiTaxonomyPlanMapped    WikiTaxonomyPlanState = "mapped"
	WikiTaxonomyPlanAbandoned WikiTaxonomyPlanState = "abandoned"
)

// WikiIngestWorkUnit is the canonical, attempt-independent map result for one
// immutable source revision and generation contract.
type WikiIngestWorkUnit struct {
	WorkID                string                  `gorm:"type:varchar(64);primaryKey" json:"work_id"`
	TenantID              uint64                  `gorm:"not null;index:idx_wiki_work_source,priority:1" json:"tenant_id"`
	KnowledgeBaseID       string                  `gorm:"type:varchar(36);not null;index:idx_wiki_work_source,priority:2" json:"knowledge_base_id"`
	KnowledgeID           string                  `gorm:"type:varchar(36);not null;index:idx_wiki_work_source,priority:3" json:"knowledge_id"`
	SourceRevisionDigest  string                  `gorm:"type:varchar(64);not null" json:"source_revision_digest"`
	SourceDocumentKey     string                  `gorm:"type:varchar(64);not null" json:"source_document_key"`
	GenerationContractKey string                  `gorm:"type:varchar(64);not null" json:"generation_contract_key"`
	RuntimeSnapshotKey    string                  `gorm:"type:varchar(64);not null" json:"runtime_snapshot_key"`
	State                 WikiIngestWorkUnitState `gorm:"type:varchar(16);not null" json:"state"`
	MappedOutput          JSON                    `gorm:"type:json;not null" json:"mapped_output"`
	CreatedAt             time.Time               `json:"created_at"`
	UpdatedAt             time.Time               `json:"updated_at"`
}

// WikiIngestWorkBinding pins one attempt's durable Wiki owner to the first
// prepared work unit. Runtime/config changes must not silently fork completed
// model work during a partial retry.
type WikiIngestWorkBinding struct {
	KnowledgeID           string `json:"-"`
	Attempt               int    `json:"-"`
	SpanID                string `json:"-"`
	WorkID                string `json:"work_id"`
	SourceRevisionDigest  string `json:"source_revision_digest"`
	SourceDocumentKey     string `json:"source_document_key"`
	GenerationContractKey string `json:"generation_contract_key"`
	RuntimeSnapshotKey    string `json:"runtime_snapshot_key"`
}

// WikiSlugApplication records the deterministic reduce plan and generated
// output for one slug and an ordered set of work-unit contributions.
type WikiSlugApplication struct {
	PlanID           string                   `gorm:"type:varchar(64);primaryKey" json:"plan_id"`
	TenantID         uint64                   `gorm:"not null;index" json:"tenant_id"`
	KnowledgeBaseID  string                   `gorm:"type:varchar(36);not null;index" json:"knowledge_base_id"`
	Slug             string                   `gorm:"type:varchar(255);not null;index" json:"slug"`
	ContributionKey  string                   `gorm:"type:varchar(64);not null;index" json:"contribution_key"`
	ExpectedVersion  int                      `gorm:"not null" json:"expected_version"`
	ExpectedPageHash string                   `gorm:"type:varchar(64);not null" json:"expected_page_hash"`
	OperationDigest  string                   `gorm:"type:varchar(64);not null" json:"operation_digest"`
	State            WikiSlugApplicationState `gorm:"type:varchar(16);not null" json:"state"`
	GeneratedOutput  string                   `gorm:"type:text;not null" json:"generated_output"`
	CreatedAt        time.Time                `json:"created_at"`
	UpdatedAt        time.Time                `json:"updated_at"`
}

// WikiTaxonomyPlan stores the resolved directory assignment before folders are
// materialized. Its identity includes work units, missing slugs and the folder
// snapshot, so a replay can skip the taxonomy model without hiding base drift.
type WikiTaxonomyPlan struct {
	PlanID           string                `gorm:"type:varchar(64);primaryKey" json:"plan_id"`
	TenantID         uint64                `gorm:"not null;index" json:"tenant_id"`
	KnowledgeBaseID  string                `gorm:"type:varchar(36);not null;index" json:"knowledge_base_id"`
	WorkSetDigest    string                `gorm:"type:varchar(64);not null" json:"work_set_digest"`
	MissingSetDigest string                `gorm:"type:varchar(64);not null" json:"missing_set_digest"`
	FolderBaseDigest string                `gorm:"type:varchar(64);not null" json:"folder_base_digest"`
	ContractKey      string                `gorm:"type:varchar(64);not null" json:"contract_key"`
	State            WikiTaxonomyPlanState `gorm:"type:varchar(16);not null" json:"state"`
	ResolvedOutput   JSON                  `gorm:"type:json;not null" json:"resolved_output"`
	CreatedAt        time.Time             `json:"created_at"`
	UpdatedAt        time.Time             `json:"updated_at"`
}

// WikiSlugContributionMarker is the exact-once publication marker for one
// document work-unit contribution to a slug operation.
type WikiSlugContributionMarker struct {
	ID              int64                    `gorm:"primaryKey;autoIncrement" json:"id"`
	PlanID          string                   `gorm:"type:varchar(64);not null;index" json:"plan_id"`
	WorkID          string                   `gorm:"type:varchar(64);not null;uniqueIndex:uq_wiki_slug_contribution,priority:1" json:"work_id"`
	Slug            string                   `gorm:"type:varchar(255);not null;uniqueIndex:uq_wiki_slug_contribution,priority:2" json:"slug"`
	OperationDigest string                   `gorm:"type:varchar(64);not null;uniqueIndex:uq_wiki_slug_contribution,priority:3" json:"operation_digest"`
	State           WikiSlugApplicationState `gorm:"type:varchar(16);not null" json:"state"`
	CreatedAt       time.Time                `json:"created_at"`
	UpdatedAt       time.Time                `json:"updated_at"`
}

type WikiSlugApplicationTransition struct {
	PlanID  string
	State   WikiSlugApplicationState
	Markers []WikiSlugContributionMarker
}

type wikiSlugApplicationTransitionContextKey struct{}

func WithWikiSlugApplicationTransition(ctx context.Context, transition WikiSlugApplicationTransition) context.Context {
	return context.WithValue(ctx, wikiSlugApplicationTransitionContextKey{}, transition)
}

func WikiSlugApplicationTransitionFromContext(ctx context.Context) (WikiSlugApplicationTransition, bool) {
	if ctx == nil {
		return WikiSlugApplicationTransition{}, false
	}
	transition, ok := ctx.Value(wikiSlugApplicationTransitionContextKey{}).(WikiSlugApplicationTransition)
	return transition, ok && transition.PlanID != "" && len(transition.Markers) > 0
}
