package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/agent"
	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type wikiGenerationLedgerPageService struct {
	interfaces.WikiPageService
	mu           sync.Mutex
	fragment     *types.WikiGenerationFragment
	reservations int
	completions  int
	ambiguous    int
	completeErr  error
}

func (s *wikiGenerationLedgerPageService) ReserveWikiGenerationFragment(
	_ context.Context, candidate *types.WikiGenerationFragment, callID string, leaseUntil time.Time, _ int,
) (*types.WikiGenerationFragment, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reservations++
	if s.fragment != nil && (s.fragment.State == types.WikiGenerationFragmentGenerated || s.fragment.State == types.WikiGenerationFragmentSucceeded) {
		copy := *s.fragment
		return &copy, false, nil
	}
	copy := *candidate
	copy.State, copy.Attempts, copy.CallID, copy.LeaseUntil = types.WikiGenerationFragmentCalling, 1, callID, &leaseUntil
	s.fragment = &copy
	return &copy, true, nil
}

func (s *wikiGenerationLedgerPageService) CompleteWikiGenerationFragment(_ context.Context, fragmentID, callID, output string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completions++
	if s.completeErr != nil {
		return s.completeErr
	}
	if s.fragment == nil || s.fragment.FragmentID != fragmentID || s.fragment.CallID != callID {
		return errors.New("owner mismatch")
	}
	s.fragment.State, s.fragment.Output = types.WikiGenerationFragmentGenerated, output
	return nil
}

func (s *wikiGenerationLedgerPageService) ReleaseWikiGenerationFragment(context.Context, string, string, string, bool) error {
	return nil
}

func (s *wikiGenerationLedgerPageService) MarkWikiGenerationFragmentAmbiguous(context.Context, string, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ambiguous++
	return nil
}

func (s *wikiGenerationLedgerPageService) ListWikiGenerationFragments(context.Context, string) ([]types.WikiGenerationFragment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fragment == nil {
		return nil, nil
	}
	return []types.WikiGenerationFragment{*s.fragment}, nil
}

func (s *wikiGenerationLedgerPageService) MarkWikiGenerationFragmentsSucceeded(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fragment != nil && s.fragment.State == types.WikiGenerationFragmentGenerated {
		s.fragment.State = types.WikiGenerationFragmentSucceeded
	}
	return nil
}

func TestGenerateWithTemplateReusesDurableGeneratedFragmentWithoutModelCall(t *testing.T) {
	pages := &wikiGenerationLedgerPageService{fragment: &types.WikiGenerationFragment{
		FragmentID: "stored", State: types.WikiGenerationFragmentGenerated,
		Output: "SUMMARY: stable\n\nbody",
	}}
	model := &scriptedWikiChat{}
	service := &wikiIngestService{wikiService: pages}
	ctx := withWikiGenerationScope(context.Background(), wikiGenerationScope{
		TenantID: 7, KnowledgeBaseID: "kb", WorkRevision: "work", RuntimeSnapshot: "runtime",
	})
	output, err := service.generateWithTemplate(ctx, model, agent.WikiSummaryPrompt, map[string]string{
		"Content": "source", "Language": "Chinese", "PageTitle": "title",
	})
	require.NoError(t, err)
	require.Equal(t, "SUMMARY: stable\n\nbody", output)
	require.Zero(t, model.streamCalls)
	require.Equal(t, 1, pages.reservations)
}

func TestGenerateWithTemplatePersistsOutputBeforeReturning(t *testing.T) {
	pages := &wikiGenerationLedgerPageService{}
	model := &scriptedWikiChat{events: []types.StreamResponse{{
		ResponseType: types.ResponseTypeAnswer, Content: "SUMMARY: stable\n\nbody", Done: true, FinishReason: "stop",
	}}}
	service := &wikiIngestService{wikiService: pages}
	ctx := withWikiGenerationScope(context.Background(), wikiGenerationScope{
		TenantID: 7, KnowledgeBaseID: "kb", WorkRevision: "work", RuntimeSnapshot: "runtime",
	})
	output, err := service.generateWithTemplate(ctx, model, agent.WikiSummaryPrompt, map[string]string{
		"Content": "source", "Language": "Chinese", "PageTitle": "title",
	})
	require.NoError(t, err)
	require.Equal(t, "SUMMARY: stable\n\nbody", output)
	require.Equal(t, 1, model.streamCalls)
	require.Equal(t, 1, pages.completions)
	require.Equal(t, types.WikiGenerationFragmentGenerated, pages.fragment.State)
}

func TestGenerateWithTemplateUsesExactRedisFallbackWhenDBOutputWriteFails(t *testing.T) {
	redisServer, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(redisServer.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	pages := &wikiGenerationLedgerPageService{completeErr: errors.New("injected DB output failure")}
	model := &scriptedWikiChat{events: []types.StreamResponse{{
		ResponseType: types.ResponseTypeAnswer, Content: "SUMMARY: stable\n\nbody", Done: true, FinishReason: "stop",
	}}}
	service := &wikiIngestService{wikiService: pages, redisClient: redisClient}
	ctx := withWikiGenerationScope(context.Background(), wikiGenerationScope{
		TenantID: 7, KnowledgeBaseID: "kb", WorkRevision: "work", RuntimeSnapshot: "runtime",
	})
	data := map[string]string{"Content": "source", "Language": "Chinese", "PageTitle": "title"}
	output, err := service.generateWithTemplate(ctx, model, agent.WikiSummaryPrompt, data)
	require.NoError(t, err)
	require.Equal(t, "SUMMARY: stable\n\nbody", output)
	require.Equal(t, 1, model.streamCalls)
	require.Len(t, redisServer.DB(0).Keys(), 1)

	pages.completeErr = nil
	require.NoError(t, service.markWikiGenerationFragmentsSucceeded(ctx, "work"))
	require.Empty(t, redisServer.DB(0).Keys())
	require.Equal(t, types.WikiGenerationFragmentSucceeded, pages.fragment.State)
	replayed, err := service.generateWithTemplate(ctx, model, agent.WikiSummaryPrompt, data)
	require.NoError(t, err)
	require.Equal(t, output, replayed)
	require.Equal(t, 1, model.streamCalls, "Redis recovery must not repeat the paid call")
	require.Equal(t, types.WikiGenerationFragmentSucceeded, pages.fragment.State)
}

func TestGenerateWithTemplateFailsAmbiguousWhenDBAndRedisCannotPersistOutput(t *testing.T) {
	redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	require.NoError(t, redisClient.Close())
	pages := &wikiGenerationLedgerPageService{completeErr: errors.New("injected DB output failure")}
	model := &scriptedWikiChat{events: []types.StreamResponse{{
		ResponseType: types.ResponseTypeAnswer, Content: "SUMMARY: stable\n\nbody", Done: true, FinishReason: "stop",
	}}}
	service := &wikiIngestService{wikiService: pages, redisClient: redisClient}
	ctx := withWikiGenerationScope(context.Background(), wikiGenerationScope{
		TenantID: 7, KnowledgeBaseID: "kb", WorkRevision: "work", RuntimeSnapshot: "runtime",
	})
	_, err := service.generateWithTemplate(ctx, model, agent.WikiSummaryPrompt, map[string]string{
		"Content": "source", "Language": "Chinese", "PageTitle": "title",
	})
	require.Error(t, err)
	require.Equal(t, WikiGenerationErrorAmbiguousCall, wikiGenerationErrorClassOf(err))
	require.Equal(t, 1, model.streamCalls)
	require.Equal(t, 1, pages.ambiguous)
}

func TestGenerateWithTemplateReusesFragmentAcrossServiceRestart(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.WikiGenerationFragment{}))
	pages := NewWikiPageService(apprepo.NewWikiPageRepository(db), nil, nil, nil, nil)
	ctx := withWikiGenerationScope(context.Background(), wikiGenerationScope{
		TenantID: 7, KnowledgeBaseID: "kb", WorkRevision: "restart-work", RuntimeSnapshot: "runtime",
	})
	data := map[string]string{"Content": "source", "Language": "Chinese", "PageTitle": "title"}
	firstModel := &scriptedWikiChat{events: []types.StreamResponse{{
		ResponseType: types.ResponseTypeAnswer, Content: "SUMMARY: stable\n\nbody", Done: true, FinishReason: "stop",
	}}}
	first, err := (&wikiIngestService{wikiService: pages}).generateWithTemplate(
		ctx, firstModel, agent.WikiSummaryPrompt, data,
	)
	require.NoError(t, err)
	require.Equal(t, 1, firstModel.streamCalls)

	secondModel := &scriptedWikiChat{}
	second, err := (&wikiIngestService{wikiService: pages}).generateWithTemplate(
		ctx, secondModel, agent.WikiSummaryPrompt, data,
	)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Zero(t, secondModel.streamCalls, "a new service process must reuse the durable generated output")
	var fragments int64
	require.NoError(t, db.Model(&types.WikiGenerationFragment{}).Count(&fragments).Error)
	require.EqualValues(t, 1, fragments)
}
