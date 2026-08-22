package handler

import (
	"context"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

type compactKnowledgeTimelineRepository interface {
	ListTimelineByAttempt(context.Context, string, int) ([]types.KnowledgeProcessingSpan, error)
	ListTokenUsageByAttempt(context.Context, string, int) ([]types.KnowledgeProcessingSpan, error)
}

// compactKnowledgeTimelineRows replaces successful high-cardinality fan-out
// rows with stable summary nodes. The latest row for each logical batch wins;
// failed and live rows remain exact so retry controls still target real spans.
func compactKnowledgeTimelineRows(rows []types.KnowledgeProcessingSpan) []types.KnowledgeProcessingSpan {
	latest := make(map[string]int)
	for i := range rows {
		if family := knowledgeTimelineFanoutFamily(rows[i].Name); family != "" {
			latest[timelineFanoutRowKey(rows[i])] = i
		}
	}

	result := make([]types.KnowledgeProcessingSpan, 0, len(rows))
	groupIndexes := make(map[string]int)
	for i := range rows {
		row := rows[i]
		if row.Kind == types.SpanKindGeneration && row.Status == types.SpanStatusDone {
			continue
		}
		family := knowledgeTimelineFanoutFamily(row.Name)
		if family == "" {
			result = append(result, row)
			continue
		}
		if latest[timelineFanoutRowKey(row)] != i {
			continue
		}
		if row.Status != types.SpanStatusDone && row.Status != types.SpanStatusSkipped {
			result = append(result, row)
			continue
		}

		groupKey := row.ParentSpanID + "\x00" + family
		groupIndex, exists := groupIndexes[groupKey]
		if !exists {
			groupIndex = len(result)
			groupIndexes[groupKey] = groupIndex
			result = append(result, types.KnowledgeProcessingSpan{
				KnowledgeID:  row.KnowledgeID,
				Attempt:      row.Attempt,
				SpanID:       "virtual:" + strings.ReplaceAll(family, ".", "_") + ":" + row.ParentSpanID,
				ParentSpanID: row.ParentSpanID,
				Name:         family,
				Kind:         types.SpanKindSubSpan,
				Status:       types.SpanStatusSkipped,
				Input:        types.JSONMap{"task_count": 0},
				Output: types.JSONMap{
					"completed_count": 0,
					"skipped_count":   0,
				},
				CreatedAt: row.CreatedAt,
				UpdatedAt: row.UpdatedAt,
			})
		}
		updateKnowledgeTimelineGroup(&result[groupIndex], row)
	}
	return result
}

func knowledgeTimelineFanoutFamily(name string) string {
	switch {
	case hasIndexedStageSuffix(name, "postprocess.graph.chunk"):
		return "postprocess.graph"
	case hasIndexedStageSuffix(name, "postprocess.question.batch"):
		return "postprocess.question.batches"
	default:
		return ""
	}
}

func timelineFanoutRowKey(row types.KnowledgeProcessingSpan) string {
	return row.ParentSpanID + "\x00" + row.Name
}

func updateKnowledgeTimelineGroup(group *types.KnowledgeProcessingSpan, row types.KnowledgeProcessingSpan) {
	group.Input["task_count"] = group.Input["task_count"].(int) + 1
	if row.Status == types.SpanStatusDone {
		group.Status = types.SpanStatusDone
		group.Output["completed_count"] = group.Output["completed_count"].(int) + 1
	} else {
		group.Output["skipped_count"] = group.Output["skipped_count"].(int) + 1
	}
	group.StartedAt = earlierTimelineTime(group.StartedAt, row.StartedAt)
	group.FinishedAt = laterTimelineTime(group.FinishedAt, row.FinishedAt)
	if group.StartedAt != nil && group.FinishedAt != nil {
		group.DurationMs = max(0, group.FinishedAt.Sub(*group.StartedAt).Milliseconds())
	}
	if row.CreatedAt.Before(group.CreatedAt) || group.CreatedAt.IsZero() {
		group.CreatedAt = row.CreatedAt
	}
	if row.UpdatedAt.After(group.UpdatedAt) {
		group.UpdatedAt = row.UpdatedAt
	}
}

func earlierTimelineTime(left, right *time.Time) *time.Time {
	if left == nil {
		return right
	}
	if right != nil && right.Before(*left) {
		return right
	}
	return left
}

func laterTimelineTime(left, right *time.Time) *time.Time {
	if left == nil {
		return right
	}
	if right != nil && right.After(*left) {
		return right
	}
	return left
}
