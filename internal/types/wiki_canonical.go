package types

import (
	"strings"
	"time"
	"unicode"
)

// NormalizeWikiIdentityTitle removes presentation-only differences while
// retaining every letter and number. It is intentionally stricter than fuzzy
// semantic deduplication.
func NormalizeWikiIdentityTitle(value string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// WikiCanonicalCandidate is an ingest proposal that needs a durable canonical
// slug before reduce starts.
type WikiCanonicalCandidate struct {
	Slug     string
	Title    string
	PageType string
}

// WikiCanonicalIdentity is the durable identity registry. Its unique key is
// what makes workers, retries, and later batches converge after Redis expires.
type WikiCanonicalIdentity struct {
	ID              int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	TenantID        uint64    `gorm:"not null;index" json:"tenant_id"`
	KnowledgeBaseID string    `gorm:"type:varchar(36);not null;uniqueIndex:uq_wiki_canonical_identity" json:"knowledge_base_id"`
	PageType        string    `gorm:"type:varchar(32);not null;uniqueIndex:uq_wiki_canonical_identity" json:"page_type"`
	IdentityKey     string    `gorm:"type:varchar(512);not null;uniqueIndex:uq_wiki_canonical_identity" json:"identity_key"`
	CanonicalSlug   string    `gorm:"type:varchar(255);not null" json:"canonical_slug"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func (WikiCanonicalIdentity) TableName() string { return "wiki_canonical_identities" }

// WikiPageMergeAudit records every automatically archived duplicate. The
// merged page and all of its revisions remain available for audit/recovery.
type WikiPageMergeAudit struct {
	ID              int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	TenantID        uint64    `gorm:"not null;index" json:"tenant_id"`
	KnowledgeBaseID string    `gorm:"type:varchar(36);not null;index" json:"knowledge_base_id"`
	PageType        string    `gorm:"type:varchar(32);not null" json:"page_type"`
	IdentityKey     string    `gorm:"type:varchar(512);not null" json:"identity_key"`
	CanonicalPageID string    `gorm:"type:varchar(36);not null" json:"canonical_page_id"`
	CanonicalSlug   string    `gorm:"type:varchar(255);not null" json:"canonical_slug"`
	MergedPageID    string    `gorm:"type:varchar(36);not null;uniqueIndex" json:"merged_page_id"`
	MergedSlug      string    `gorm:"type:varchar(255);not null" json:"merged_slug"`
	Reason          string    `gorm:"type:varchar(64);not null" json:"reason"`
	CreatedAt       time.Time `json:"created_at"`
}

func (WikiPageMergeAudit) TableName() string { return "wiki_page_merge_audits" }

type WikiCanonicalReconcileResult struct {
	MergedPages   int
	DeferredPages int
	Aliases       map[string]string
}
