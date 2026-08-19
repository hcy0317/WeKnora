package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func validateWikiGenerationFragment(fragment *types.WikiGenerationFragment) error {
	if fragment == nil || strings.TrimSpace(fragment.FragmentID) == "" || fragment.TenantID == 0 ||
		strings.TrimSpace(fragment.KnowledgeBaseID) == "" || strings.TrimSpace(fragment.WorkRevision) == "" ||
		strings.TrimSpace(fragment.Purpose) == "" || strings.TrimSpace(fragment.FragmentKey) == "" ||
		strings.TrimSpace(fragment.PromptDigest) == "" || strings.TrimSpace(fragment.ModelSnapshot) == "" {
		return errors.New("reserve wiki generation fragment: complete identity is required")
	}
	return nil
}

func sameWikiGenerationFragmentIdentity(left, right *types.WikiGenerationFragment) bool {
	return left != nil && right != nil && left.FragmentID == right.FragmentID &&
		left.TenantID == right.TenantID && left.KnowledgeBaseID == right.KnowledgeBaseID &&
		left.WorkRevision == right.WorkRevision && left.Purpose == right.Purpose &&
		left.FragmentKey == right.FragmentKey && left.PromptDigest == right.PromptDigest &&
		left.ModelSnapshot == right.ModelSnapshot
}

func (r *wikiPageRepository) ReserveWikiGenerationFragment(
	ctx context.Context,
	candidate *types.WikiGenerationFragment,
	callID string,
	leaseUntil time.Time,
	maxAttempts int,
) (*types.WikiGenerationFragment, bool, error) {
	if err := validateWikiGenerationFragment(candidate); err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(callID) == "" || maxAttempts < 1 || !leaseUntil.After(time.Now()) {
		return nil, false, errors.New("reserve wiki generation fragment: call id, future lease and budget are required")
	}
	prepared := *candidate
	prepared.State = types.WikiGenerationFragmentReady
	prepared.Attempts = 0
	prepared.CallID = ""
	prepared.LeaseUntil = nil
	prepared.Output = ""
	prepared.LastError = ""

	var stored types.WikiGenerationFragment
	granted := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", prepared.FragmentID).Error; err != nil {
				return err
			}
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&prepared).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("fragment_id = ?", prepared.FragmentID).First(&stored).Error; err != nil {
			return err
		}
		if !sameWikiGenerationFragmentIdentity(&stored, &prepared) {
			return errors.New("reserve wiki generation fragment: stored identity differs")
		}
		now := time.Now()
		switch stored.State {
		case types.WikiGenerationFragmentReady:
			if stored.Attempts >= maxAttempts {
				if err := tx.Model(&stored).Updates(map[string]any{
					"state":   types.WikiGenerationFragmentTerminal,
					"call_id": "", "lease_until": nil,
				}).Error; err != nil {
					return err
				}
				stored.State = types.WikiGenerationFragmentTerminal
				return nil
			}
			stored.State = types.WikiGenerationFragmentCalling
			stored.Attempts++
			stored.CallID = callID
			stored.LeaseUntil = &leaseUntil
			stored.LastError = ""
			if err := tx.Model(&stored).Updates(map[string]any{
				"state": stored.State, "attempts": stored.Attempts,
				"call_id": callID, "lease_until": leaseUntil, "last_error": "",
			}).Error; err != nil {
				return err
			}
			granted = true
			return nil
		case types.WikiGenerationFragmentCalling:
			if stored.LeaseUntil == nil || !stored.LeaseUntil.After(now) {
				stored.State = types.WikiGenerationFragmentAmbiguous
				stored.LastError = "calling lease expired before a durable output was recorded"
				if err := tx.Model(&stored).Updates(map[string]any{
					"state": stored.State, "last_error": stored.LastError,
					"call_id": "", "lease_until": nil,
				}).Error; err != nil {
					return err
				}
			}
			return nil
		case types.WikiGenerationFragmentGenerated,
			types.WikiGenerationFragmentSucceeded,
			types.WikiGenerationFragmentTerminal,
			types.WikiGenerationFragmentAmbiguous:
			return nil
		default:
			return fmt.Errorf("reserve wiki generation fragment: unknown state %q", stored.State)
		}
	})
	if err != nil {
		return nil, false, err
	}
	return &stored, granted, nil
}

func (r *wikiPageRepository) CompleteWikiGenerationFragment(
	ctx context.Context, fragmentID, callID, output string,
) error {
	if fragmentID == "" || callID == "" || strings.TrimSpace(output) == "" {
		return errors.New("complete wiki generation fragment: owner and output are required")
	}
	result := r.db.WithContext(ctx).Model(&types.WikiGenerationFragment{}).
		Where("fragment_id = ? AND state = ? AND call_id = ?", fragmentID, types.WikiGenerationFragmentCalling, callID).
		Updates(map[string]any{
			"state": types.WikiGenerationFragmentGenerated, "output": output,
			"call_id": "", "lease_until": nil, "last_error": "",
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("complete wiki generation fragment: owner fence rejected")
	}
	return nil
}

func (r *wikiPageRepository) ReleaseWikiGenerationFragment(
	ctx context.Context, fragmentID, callID, lastError string, terminal bool,
) error {
	state := types.WikiGenerationFragmentReady
	if terminal {
		state = types.WikiGenerationFragmentTerminal
	}
	result := r.db.WithContext(ctx).Model(&types.WikiGenerationFragment{}).
		Where("fragment_id = ? AND state = ? AND call_id = ?", fragmentID, types.WikiGenerationFragmentCalling, callID).
		Updates(map[string]any{
			"state": state, "call_id": "", "lease_until": nil, "last_error": lastError,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("release wiki generation fragment: owner fence rejected")
	}
	return nil
}

func (r *wikiPageRepository) MarkWikiGenerationFragmentAmbiguous(
	ctx context.Context, fragmentID, callID, lastError string,
) error {
	result := r.db.WithContext(ctx).Model(&types.WikiGenerationFragment{}).
		Where("fragment_id = ? AND state = ? AND call_id = ?", fragmentID, types.WikiGenerationFragmentCalling, callID).
		Updates(map[string]any{
			"state":   types.WikiGenerationFragmentAmbiguous,
			"call_id": "", "lease_until": nil, "last_error": lastError,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("mark wiki generation fragment ambiguous: owner fence rejected")
	}
	return nil
}

func (r *wikiPageRepository) ListWikiGenerationFragments(
	ctx context.Context, workRevision string,
) ([]types.WikiGenerationFragment, error) {
	if strings.TrimSpace(workRevision) == "" {
		return nil, errors.New("list wiki generation fragments: work revision is required")
	}
	var fragments []types.WikiGenerationFragment
	err := r.db.WithContext(ctx).
		Where("work_revision = ?", workRevision).
		Order("fragment_id ASC").Find(&fragments).Error
	return fragments, err
}

func (r *wikiPageRepository) MarkWikiGenerationFragmentsSucceeded(ctx context.Context, workRevision string) error {
	if strings.TrimSpace(workRevision) == "" {
		return errors.New("mark wiki generation fragments succeeded: work revision is required")
	}
	return r.db.WithContext(ctx).Model(&types.WikiGenerationFragment{}).
		Where("work_revision = ? AND state = ?", workRevision, types.WikiGenerationFragmentGenerated).
		Update("state", types.WikiGenerationFragmentSucceeded).Error
}
