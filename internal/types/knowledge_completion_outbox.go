package types

import "time"

type KnowledgeCompletionOutboxState string

const (
	KnowledgeCompletionOutboxPending   KnowledgeCompletionOutboxState = "pending"
	KnowledgeCompletionOutboxCompleted KnowledgeCompletionOutboxState = "completed"
)

// KnowledgeCompletionOutbox durably couples a successful knowledge attempt
// with the targeted removal of stale archived runtime-task records. The
// business completion and this row are committed in the same transaction.
type KnowledgeCompletionOutbox struct {
	ID               int64                          `gorm:"primaryKey;autoIncrement" json:"id"`
	KnowledgeID      string                         `gorm:"type:varchar(64);not null;uniqueIndex:uq_knowledge_completion_attempt,priority:1" json:"knowledge_id"`
	Attempt          int                            `gorm:"not null;uniqueIndex:uq_knowledge_completion_attempt,priority:2" json:"attempt"`
	State            KnowledgeCompletionOutboxState `gorm:"type:varchar(16);not null;default:pending;index" json:"state"`
	DeletedArchived  int                            `gorm:"not null;default:0" json:"deleted_archived"`
	ClassifiedLegacy int                            `gorm:"not null;default:0" json:"classified_legacy"`
	LastError        string                         `gorm:"type:text;not null;default:''" json:"last_error"`
	CreatedAt        time.Time                      `json:"created_at"`
	UpdatedAt        time.Time                      `json:"updated_at"`
	CompletedAt      *time.Time                     `json:"completed_at,omitempty"`
}

func (KnowledgeCompletionOutbox) TableName() string {
	return "knowledge_completion_outbox"
}
