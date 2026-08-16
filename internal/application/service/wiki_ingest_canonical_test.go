package service

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestCanonicalizeBatchSlugUpdatesMergesConcurrentDocumentVariants(t *testing.T) {
	updates := map[string][]SlugUpdate{
		"entity/tong-yu-609": {{
			Slug: "entity/tong-yu-609", Type: "entity", KnowledgeID: "doc-a",
			Item: extractedItem{Name: "同玉609", Slug: "entity/tong-yu-609", SourceChunks: []string{"a1"}},
		}},
		"entity/tongyu-609": {{
			Slug: "entity/tongyu-609", Type: "entity", KnowledgeID: "doc-b",
			Item: extractedItem{Name: "同玉609", Slug: "entity/tongyu-609", SourceChunks: []string{"b1"}},
		}},
		"summary/doc-a": {{
			Slug: "summary/doc-a", Type: "summary", KnowledgeID: "doc-a",
			SummaryBody: "采用 [[entity/tong-yu-609|同玉609]] 进行试验。",
		}},
	}

	got, aliases := canonicalizeBatchSlugUpdates(updates, map[string]bool{
		"entity/tongyu-609": true,
	})

	if aliases["entity/tong-yu-609"] != "entity/tongyu-609" {
		t.Fatalf("alias map = %v, want fresh variant mapped to existing slug", aliases)
	}
	entityUpdates := got["entity/tongyu-609"]
	if len(entityUpdates) != 2 {
		t.Fatalf("canonical updates = %+v, want two document contributions", entityUpdates)
	}
	for _, update := range entityUpdates {
		if update.Slug != "entity/tongyu-609" || update.Item.Slug != "entity/tongyu-609" {
			t.Fatalf("update not rewritten to canonical slug: %+v", update)
		}
	}
	summary := got["summary/doc-a"]
	if len(summary) != 1 || summary[0].SummaryBody != "采用 [[entity/tongyu-609|同玉609]] 进行试验。" {
		t.Fatalf("summary links were not rewritten: %+v", summary)
	}
}

func TestCanonicalizeBatchSlugUpdatesKeepsRelatedDistinctSubjects(t *testing.T) {
	updates := map[string][]SlugUpdate{
		"concept/yumi-wenku-bing": {{
			Slug: "concept/yumi-wenku-bing", Type: "concept",
			Item: extractedItem{Name: "玉米纹枯病", Slug: "concept/yumi-wenku-bing"},
		}},
		"concept/wenku-bing": {{
			Slug: "concept/wenku-bing", Type: "concept",
			Item: extractedItem{Name: "纹枯病", Slug: "concept/wenku-bing"},
		}},
	}

	got, aliases := canonicalizeBatchSlugUpdates(updates, nil)
	if len(got) != 2 || len(aliases) != 0 {
		t.Fatalf("got %d groups aliases=%v, want two distinct subjects", len(got), aliases)
	}
}

func TestRewriteDocResultSlugsUsesBatchCanonicalMapping(t *testing.T) {
	results := []*docIngestResult{{
		Summary: "采用 [[entity/tong-yu-609|同玉609]]。",
		Pages: []wikiIngestPageRef{
			{Slug: "entity/tong-yu-609", Title: "同玉609"},
			{Slug: "entity/tongyu-609", Title: "同玉609"},
			{Slug: "summary/doc-a", Title: "文档A"},
		},
	}}

	rewriteDocResultSlugs(results, map[string]string{
		"entity/tong-yu-609": "entity/tongyu-609",
	})

	if len(results[0].Pages) != 2 {
		t.Fatalf("pages = %+v, want canonical entity plus summary", results[0].Pages)
	}
	if results[0].Pages[0].Slug != "entity/tongyu-609" {
		t.Fatalf("first page = %+v, want canonical slug", results[0].Pages[0])
	}
	if results[0].Summary != "采用 [[entity/tongyu-609|同玉609]]。" {
		t.Fatalf("summary = %q, want canonical link", results[0].Summary)
	}
}

func TestReserveConcurrentCanonicalAliasesCoordinatesDifferentBatches(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	svc := &wikiIngestService{redisClient: client}

	first := map[string][]SlugUpdate{
		"entity/tongyu-609": {{
			Slug: "entity/tongyu-609", Type: "entity",
			Item: extractedItem{Name: "同玉609", Slug: "entity/tongyu-609"},
		}},
	}
	if aliases := svc.reserveConcurrentCanonicalAliases(context.Background(), "kb-1", first, nil); len(aliases) != 0 {
		t.Fatalf("first reservation aliases = %v, want first writer canonical", aliases)
	}

	second := map[string][]SlugUpdate{
		"entity/tong-yu-609": {{
			Slug: "entity/tong-yu-609", Type: "entity",
			Item: extractedItem{Name: "Tongyu 609", Slug: "entity/tong-yu-609"},
		}},
	}
	aliases := svc.reserveConcurrentCanonicalAliases(context.Background(), "kb-1", second, nil)
	if aliases["entity/tong-yu-609"] != "entity/tongyu-609" {
		t.Fatalf("second reservation aliases = %v, want convergence on first batch slug", aliases)
	}

	// A page confirmed in the database is authoritative over a stale
	// in-flight reservation, and refreshes the reservation for later batches.
	if aliases := svc.reserveConcurrentCanonicalAliases(context.Background(), "kb-1", second, map[string]bool{
		"entity/tong-yu-609": true,
	}); len(aliases) != 0 {
		t.Fatalf("existing page reservation aliases = %v, want existing slug retained", aliases)
	}
	aliases = svc.reserveConcurrentCanonicalAliases(context.Background(), "kb-1", first, nil)
	if aliases["entity/tongyu-609"] != "entity/tong-yu-609" {
		t.Fatalf("refreshed reservation aliases = %v, want confirmed existing slug", aliases)
	}
}
