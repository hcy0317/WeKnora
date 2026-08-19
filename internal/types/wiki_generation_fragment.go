package types

import "time"

type WikiGenerationFragmentState string

const (
	WikiGenerationFragmentReady     WikiGenerationFragmentState = "ready"
	WikiGenerationFragmentCalling   WikiGenerationFragmentState = "calling"
	WikiGenerationFragmentGenerated WikiGenerationFragmentState = "generated"
	WikiGenerationFragmentSucceeded WikiGenerationFragmentState = "succeeded"
	WikiGenerationFragmentTerminal  WikiGenerationFragmentState = "terminal"
	WikiGenerationFragmentAmbiguous WikiGenerationFragmentState = "ambiguous"
)

// WikiGenerationFragment is the durable, paid-call boundary for one stable
// Wiki prompt. Attempts are shared by task redeliveries and process restarts.
type WikiGenerationFragment struct {
	FragmentID      string                      `gorm:"type:varchar(64);primaryKey" json:"fragment_id"`
	TenantID        uint64                      `gorm:"not null;uniqueIndex:uq_wiki_generation_fragment_identity,priority:1" json:"tenant_id"`
	KnowledgeBaseID string                      `gorm:"type:varchar(36);not null;uniqueIndex:uq_wiki_generation_fragment_identity,priority:2" json:"knowledge_base_id"`
	WorkRevision    string                      `gorm:"type:varchar(64);not null;uniqueIndex:uq_wiki_generation_fragment_identity,priority:3;index" json:"work_revision"`
	Purpose         string                      `gorm:"type:varchar(64);not null;uniqueIndex:uq_wiki_generation_fragment_identity,priority:4" json:"purpose"`
	FragmentKey     string                      `gorm:"type:varchar(64);not null;uniqueIndex:uq_wiki_generation_fragment_identity,priority:5" json:"fragment_key"`
	PromptDigest    string                      `gorm:"type:varchar(64);not null;uniqueIndex:uq_wiki_generation_fragment_identity,priority:6" json:"prompt_digest"`
	ModelSnapshot   string                      `gorm:"type:varchar(64);not null;uniqueIndex:uq_wiki_generation_fragment_identity,priority:7" json:"model_snapshot"`
	State           WikiGenerationFragmentState `gorm:"type:varchar(16);not null;index" json:"state"`
	Attempts        int                         `gorm:"not null;default:0" json:"attempts"`
	CallID          string                      `gorm:"type:varchar(36);not null;default:''" json:"call_id"`
	LeaseUntil      *time.Time                  `json:"lease_until,omitempty"`
	Output          string                      `gorm:"type:text;not null;default:''" json:"output"`
	LastError       string                      `gorm:"type:text;not null;default:''" json:"last_error"`
	CreatedAt       time.Time                   `json:"created_at"`
	UpdatedAt       time.Time                   `json:"updated_at"`
}
