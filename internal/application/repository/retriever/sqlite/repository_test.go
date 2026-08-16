package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gormsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var registerSQLiteVec sync.Once

func newSQLiteRetrieverTestRepository(t *testing.T) *sqliteRepository {
	t.Helper()
	registerSQLiteVec.Do(sqlite_vec.Auto)

	dbName := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	db, err := gorm.Open(gormsqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", dbName)), &gorm.Config{})
	require.NoError(t, err)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
	})

	repository, ok := NewSQLiteRetrieveEngineRepository(db).(*sqliteRepository)
	require.True(t, ok)
	return repository
}

func saveSQLiteTestVector(t *testing.T, repository *sqliteRepository, info *types.IndexInfo, embedding []float32) {
	t.Helper()
	require.NoError(t, repository.Save(context.Background(), info, map[string]any{
		"embedding": map[string][]float32{info.SourceID: embedding},
	}))
}

func sqliteTestIndex(chunkID, knowledgeBaseID, knowledgeID, tagID string, enabled bool) *types.IndexInfo {
	return &types.IndexInfo{
		Content:         chunkID,
		SourceID:        "source-" + chunkID,
		SourceType:      types.ChunkSourceType,
		ChunkID:         chunkID,
		KnowledgeID:     knowledgeID,
		KnowledgeBaseID: knowledgeBaseID,
		TagID:           tagID,
		IsEnabled:       enabled,
	}
}

func sqliteSurfaceCounts(t *testing.T, repository *sqliteRepository, info *types.IndexInfo, dim int) (int64, int64, int64) {
	t.Helper()
	var metadataCount, ftsCount, vecCount int64
	require.NoError(t, repository.db.Model(&sqliteEmbedding{}).
		Where("source_id = ? AND source_type = ?", info.SourceID, int(info.SourceType)).
		Count(&metadataCount).Error)
	require.NoError(t, repository.db.Raw("SELECT count(*) FROM lite_embeddings_fts").Scan(&ftsCount).Error)
	require.NoError(t, repository.db.Raw(
		fmt.Sprintf("SELECT count(*) FROM %s", vecTableName(dim)),
	).Scan(&vecCount).Error)
	return metadataCount, ftsCount, vecCount
}

func requireSQLiteSurfaceCounts(t *testing.T, repository *sqliteRepository, info *types.IndexInfo, dim int, want int64) {
	t.Helper()
	metadataCount, ftsCount, vecCount := sqliteSurfaceCounts(t, repository, info, dim)
	assert.Equal(t, want, metadataCount, "metadata count")
	assert.Equal(t, want, ftsCount, "FTS count")
	assert.Equal(t, want, vecCount, "vector count")
}

func failSQLiteRawSQL(t *testing.T, repository *sqliteRepository, fragment string) func() {
	t.Helper()
	name := "test:fail_raw:" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	require.NoError(t, repository.db.Callback().Raw().Before("gorm:raw").Register(name, func(db *gorm.DB) {
		if strings.Contains(db.Statement.SQL.String(), fragment) {
			db.AddError(errors.New("injected raw SQL failure"))
		}
	}))
	return func() {
		require.NoError(t, repository.db.Callback().Raw().Remove(name))
	}
}

func failSQLiteMetadataDelete(t *testing.T, repository *sqliteRepository) func() {
	t.Helper()
	name := "test:fail_delete:" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	require.NoError(t, repository.db.Callback().Delete().Before("gorm:delete").Register(name, func(db *gorm.DB) {
		db.AddError(errors.New("injected metadata delete failure"))
	}))
	return func() {
		require.NoError(t, repository.db.Callback().Delete().Remove(name))
	}
}

func TestVectorRetrieveFiltersBeforeTopK(t *testing.T) {
	testCases := []struct {
		name      string
		blocker   *types.IndexInfo
		configure func(*types.RetrieveParams)
	}{
		{
			name:    "knowledge base",
			blocker: sqliteTestIndex("blocker", "kb-other", "knowledge-target", "tag-target", true),
			configure: func(params *types.RetrieveParams) {
				params.KnowledgeBaseIDs = []string{"kb-target"}
			},
		},
		{
			name:    "knowledge",
			blocker: sqliteTestIndex("blocker", "kb-target", "knowledge-other", "tag-target", true),
			configure: func(params *types.RetrieveParams) {
				params.KnowledgeIDs = []string{"knowledge-target"}
			},
		},
		{
			name:    "tag",
			blocker: sqliteTestIndex("blocker", "kb-target", "knowledge-target", "tag-other", true),
			configure: func(params *types.RetrieveParams) {
				params.TagIDs = []string{"tag-target"}
			},
		},
		{
			name:      "enabled status",
			blocker:   sqliteTestIndex("blocker", "kb-target", "knowledge-target", "tag-target", false),
			configure: func(_ *types.RetrieveParams) {},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			repository := newSQLiteRetrieverTestRepository(t)
			saveSQLiteTestVector(t, repository, testCase.blocker, []float32{1, 0})
			saveSQLiteTestVector(t, repository,
				sqliteTestIndex("target", "kb-target", "knowledge-target", "tag-target", true),
				[]float32{0.8, 0.6},
			)

			params := types.RetrieveParams{
				Embedding:     []float32{1, 0},
				TopK:          1,
				RetrieverType: types.VectorRetrieverType,
			}
			testCase.configure(&params)

			results, err := repository.vectorRetrieve(context.Background(), params)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Len(t, results[0].Results, 1)
			assert.Equal(t, "target", results[0].Results[0].ChunkID)
		})
	}
}

func TestVectorRetrieveZeroThresholdDoesNotFilter(t *testing.T) {
	repository := newSQLiteRetrieverTestRepository(t)
	saveSQLiteTestVector(t, repository,
		sqliteTestIndex("anti-correlated", "kb-target", "knowledge-target", "tag-target", true),
		[]float32{-1, 0},
	)

	results, err := repository.vectorRetrieve(context.Background(), types.RetrieveParams{
		Embedding:        []float32{1, 0},
		KnowledgeBaseIDs: []string{"kb-target"},
		TopK:             1,
		Threshold:        0,
		RetrieverType:    types.VectorRetrieverType,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Results, 1)
	assert.Equal(t, "anti-correlated", results[0].Results[0].ChunkID)
	assert.Less(t, results[0].Results[0].Score, 0.0)
}

func TestVectorRetrieveAppliesSimilarityThreshold(t *testing.T) {
	repository := newSQLiteRetrieverTestRepository(t)
	saveSQLiteTestVector(t, repository,
		sqliteTestIndex("below-threshold", "kb-target", "knowledge-target", "tag-target", true),
		[]float32{0.5, 0.8660254},
	)

	results, err := repository.vectorRetrieve(context.Background(), types.RetrieveParams{
		Embedding:        []float32{1, 0},
		KnowledgeBaseIDs: []string{"kb-target"},
		TopK:             1,
		Threshold:        0.75,
		RetrieverType:    types.VectorRetrieverType,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Results)
}

func TestRetrieveReturnsVectorQueryError(t *testing.T) {
	repository := newSQLiteRetrieverTestRepository(t)
	saveSQLiteTestVector(t, repository,
		sqliteTestIndex("chunk", "kb-target", "knowledge-target", "tag-target", true),
		[]float32{1, 0},
	)
	require.NoError(t, repository.db.Exec("DROP TABLE "+vecTableName(2)).Error)

	results, err := repository.Retrieve(context.Background(), types.RetrieveParams{
		Embedding:     []float32{1, 0},
		TopK:          1,
		RetrieverType: types.VectorRetrieverType,
	})
	require.Error(t, err)
	assert.Nil(t, results)
	assert.Contains(t, err.Error(), "sqlite-vec query failed")
}

func TestRetrieveReturnsKeywordQueryError(t *testing.T) {
	repository := newSQLiteRetrieverTestRepository(t)
	require.NoError(t, repository.db.Exec("DROP TABLE IF EXISTS lite_embeddings_fts").Error)

	results, err := repository.Retrieve(context.Background(), types.RetrieveParams{
		Query:         "missing fts table",
		TopK:          1,
		RetrieverType: types.KeywordsRetrieverType,
	})
	require.Error(t, err)
	assert.Nil(t, results)
	assert.Contains(t, err.Error(), "FTS5 query failed")
}

func TestSaveAndBatchSaveRollbackAllSurfacesAndRetry(t *testing.T) {
	saveMethods := map[string]func(*sqliteRepository, *types.IndexInfo, map[string]any) error{
		"Save": func(repository *sqliteRepository, info *types.IndexInfo, params map[string]any) error {
			return repository.Save(context.Background(), info, params)
		},
		"BatchSave": func(repository *sqliteRepository, info *types.IndexInfo, params map[string]any) error {
			return repository.BatchSave(context.Background(), []*types.IndexInfo{info}, params)
		},
	}
	failures := map[string]string{
		"FTS":    "INSERT INTO lite_embeddings_fts",
		"vector": "INSERT INTO vec_embeddings_2",
	}

	for methodName, save := range saveMethods {
		for failureName, fragment := range failures {
			t.Run(methodName+"/"+failureName, func(t *testing.T) {
				repository := newSQLiteRetrieverTestRepository(t)
				repository.ensureVecTable(2)
				require.True(t, repository.vecTables[2])
				info := sqliteTestIndex("atomic-save", "kb", "knowledge", "tag", true)
				params := map[string]any{"embedding": map[string][]float32{info.SourceID: {1, 0}}}
				removeFailure := failSQLiteRawSQL(t, repository, fragment)

				err := save(repository, info, params)

				require.Error(t, err)
				requireSQLiteSurfaceCounts(t, repository, info, 2, 0)
				removeFailure()
				require.NoError(t, save(repository, info, params))
				requireSQLiteSurfaceCounts(t, repository, info, 2, 1)
			})
		}
	}
}

func TestSaveAndBatchSaveRepairExistingMetadataIndexes(t *testing.T) {
	saveMethods := map[string]func(*sqliteRepository, *types.IndexInfo, map[string]any) error{
		"Save": func(repository *sqliteRepository, info *types.IndexInfo, params map[string]any) error {
			return repository.Save(context.Background(), info, params)
		},
		"BatchSave": func(repository *sqliteRepository, info *types.IndexInfo, params map[string]any) error {
			return repository.BatchSave(context.Background(), []*types.IndexInfo{info}, params)
		},
	}

	for methodName, save := range saveMethods {
		t.Run(methodName, func(t *testing.T) {
			repository := newSQLiteRetrieverTestRepository(t)
			repository.ensureVecTable(2)
			require.True(t, repository.vecTables[2])
			info := sqliteTestIndex("repair-existing", "kb", "knowledge", "tag", true)
			row := toSQLiteEmbedding(info)
			row.Dimension = 2
			require.NoError(t, repository.db.Create(row).Error)
			metadataCount, ftsCount, vecCount := sqliteSurfaceCounts(t, repository, info, 2)
			assert.Equal(t, int64(1), metadataCount)
			assert.Zero(t, ftsCount)
			assert.Zero(t, vecCount)

			require.NoError(t, save(repository, info, map[string]any{
				"embedding": map[string][]float32{info.SourceID: {1, 0}},
			}))

			requireSQLiteSurfaceCounts(t, repository, info, 2, 1)
		})
	}
}

func TestDeleteMethodsRollbackAllSurfacesAndRetry(t *testing.T) {
	deleteMethods := map[string]func(*sqliteRepository, *types.IndexInfo) error{
		"chunk": func(repository *sqliteRepository, info *types.IndexInfo) error {
			return repository.DeleteByChunkIDList(context.Background(), []string{info.ChunkID}, 0, "")
		},
		"knowledge": func(repository *sqliteRepository, info *types.IndexInfo) error {
			return repository.DeleteByKnowledgeIDList(context.Background(), []string{info.KnowledgeID}, 0, "")
		},
		"source": func(repository *sqliteRepository, info *types.IndexInfo) error {
			return repository.DeleteBySourceIDList(context.Background(), []string{info.SourceID}, 0, "")
		},
	}

	for methodName, deleteMethod := range deleteMethods {
		for _, failureName := range []string{"vector", "FTS", "metadata"} {
			t.Run(methodName+"/"+failureName, func(t *testing.T) {
				repository := newSQLiteRetrieverTestRepository(t)
				info := sqliteTestIndex("atomic-delete", "kb", "knowledge", "tag", true)
				saveSQLiteTestVector(t, repository, info, []float32{1, 0})
				requireSQLiteSurfaceCounts(t, repository, info, 2, 1)

				var removeFailure func()
				switch failureName {
				case "vector":
					removeFailure = failSQLiteRawSQL(t, repository, "DELETE FROM vec_embeddings_2")
				case "FTS":
					removeFailure = failSQLiteRawSQL(t, repository, "DELETE FROM lite_embeddings_fts")
				case "metadata":
					removeFailure = failSQLiteMetadataDelete(t, repository)
				}

				err := deleteMethod(repository, info)

				require.Error(t, err)
				requireSQLiteSurfaceCounts(t, repository, info, 2, 1)
				removeFailure()
				require.NoError(t, deleteMethod(repository, info))
				requireSQLiteSurfaceCounts(t, repository, info, 2, 0)
			})
		}
	}
}

func TestBatchSavePersistsKeywordAndVectorIndices(t *testing.T) {
	repository := newSQLiteRetrieverTestRepository(t)
	info := sqliteTestIndex("batch-success", "kb", "knowledge", "tag", true)
	require.NoError(t, repository.BatchSave(context.Background(), []*types.IndexInfo{info}, map[string]any{
		"embedding": map[string][]float32{info.SourceID: {1, 0}},
	}))

	keywordResults, err := repository.Retrieve(context.Background(), types.RetrieveParams{
		Query:         "batch-success",
		TopK:          1,
		RetrieverType: types.KeywordsRetrieverType,
	})
	require.NoError(t, err)
	require.Len(t, keywordResults, 1)
	require.Len(t, keywordResults[0].Results, 1)
	assert.Equal(t, info.ChunkID, keywordResults[0].Results[0].ChunkID)

	vectorResults, err := repository.Retrieve(context.Background(), types.RetrieveParams{
		Embedding:     []float32{1, 0},
		TopK:          1,
		RetrieverType: types.VectorRetrieverType,
	})
	require.NoError(t, err)
	require.Len(t, vectorResults, 1)
	require.Len(t, vectorResults[0].Results, 1)
	assert.Equal(t, info.ChunkID, vectorResults[0].Results[0].ChunkID)
}
