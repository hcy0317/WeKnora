package neo4j

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/stretchr/testify/require"
)

func TestAddGraphRejectsUnavailableBackend(t *testing.T) {
	repository := NewNeo4jRepository(nil)
	err := repository.AddGraph(context.Background(), types.NameSpace{}, []*types.GraphData{{}})
	require.ErrorContains(t, err, "neo4j graph backend is disabled or unavailable")
}

func TestAddGraphIsolatedMutationAndIdempotency(t *testing.T) {
	uri := os.Getenv("WEKNORA_TEST_NEO4J_URI")
	if uri == "" {
		t.Skip("WEKNORA_TEST_NEO4J_URI is required for isolated Neo4j integration")
	}
	user := os.Getenv("WEKNORA_TEST_NEO4J_USER")
	if user == "" {
		user = "neo4j"
	}
	password := os.Getenv("WEKNORA_TEST_NEO4J_PASSWORD")
	if password == "" {
		password = "g005-test-password"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	driver, err := neo4jdriver.NewDriver(uri, neo4jdriver.BasicAuth(user, password, ""))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(context.Background())) })
	require.NoError(t, driver.VerifyConnectivity(ctx))

	namespace := types.NameSpace{
		KnowledgeBase: "g005_isolated",
		Knowledge:     "synthetic_" + time.Now().UTC().Format("20060102150405.000000000"),
	}
	repo := &Neo4jRepository{driver: driver, nodePrefix: "ENTITY"}
	invalidCases := []struct {
		name      string
		namespace types.NameSpace
		graph     *types.GraphData
		want      string
	}{
		{
			name:      "empty namespace",
			namespace: types.NameSpace{},
			graph:     &types.GraphData{Node: []*types.GraphNode{{Name: "Alpha"}}},
			want:      "graph namespace knowledge_base and knowledge are required",
		},
		{
			name:      "empty node name",
			namespace: types.NameSpace{KnowledgeBase: "g005_isolated", Knowledge: namespace.Knowledge + "_empty_node"},
			graph:     &types.GraphData{Node: []*types.GraphNode{{Name: "  "}}},
			want:      "graph node name is required",
		},
		{
			name:      "empty relationship source",
			namespace: types.NameSpace{KnowledgeBase: "g005_isolated", Knowledge: namespace.Knowledge + "_empty_source"},
			graph:     &types.GraphData{Relation: []*types.GraphRelation{{Node1: "", Node2: "Beta", Type: "RELATED_TO"}}},
			want:      "graph relationship source and target are required",
		},
		{
			name:      "empty relationship target",
			namespace: types.NameSpace{KnowledgeBase: "g005_isolated", Knowledge: namespace.Knowledge + "_empty_target"},
			graph:     &types.GraphData{Relation: []*types.GraphRelation{{Node1: "Alpha", Node2: " ", Type: "RELATED_TO"}}},
			want:      "graph relationship source and target are required",
		},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			err := repo.AddGraph(ctx, tc.namespace, []*types.GraphData{tc.graph})
			require.ErrorContains(t, err, tc.want)
			if tc.namespace.Knowledge != "" {
				require.Equal(t, int64(0), neo4jCount(t, ctx, driver,
					"MATCH (n {kg: $kg}) RETURN count(n) AS count", tc.namespace.Knowledge))
			}
		})
	}
	t.Cleanup(func() {
		session := driver.NewSession(context.Background(), neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite})
		defer session.Close(context.Background())
		_, cleanupErr := session.ExecuteWrite(context.Background(), func(tx neo4jdriver.ManagedTransaction) (interface{}, error) {
			_, runErr := tx.Run(context.Background(), "MATCH (n {kg: $kg}) DETACH DELETE n", map[string]interface{}{"kg": namespace.Knowledge})
			return nil, runErr
		})
		require.NoError(t, cleanupErr)
	})

	graph := &types.GraphData{
		Node: []*types.GraphNode{
			{Name: "Alpha", Chunks: []string{"chunk-a"}, Attributes: []string{"first"}},
			{Name: "Beta", Chunks: []string{"chunk-b"}, Attributes: []string{"second"}},
		},
		Relation: []*types.GraphRelation{{Node1: "Alpha", Node2: "Beta", Type: "RELATED_TO"}},
	}

	require.Equal(t, int64(0), neo4jCount(t, ctx, driver, "MATCH (n {kg: $kg}) RETURN count(n) AS count", namespace.Knowledge))
	require.NoError(t, repo.AddGraph(ctx, namespace, []*types.GraphData{graph}))
	require.Equal(t, int64(2), neo4jCount(t, ctx, driver, "MATCH (n {kg: $kg}) RETURN count(n) AS count", namespace.Knowledge))
	require.Equal(t, int64(1), neo4jCount(t, ctx, driver, "MATCH (a {kg: $kg})-[r]->(b {kg: $kg}) RETURN count(r) AS count", namespace.Knowledge))

	// MERGE identity is (labels, name, kg) for nodes and (source,type,target)
	// for relationships, so replaying an extraction is a true no-op.
	require.NoError(t, repo.AddGraph(ctx, namespace, []*types.GraphData{graph}))
	require.Equal(t, int64(2), neo4jCount(t, ctx, driver, "MATCH (n {kg: $kg}) RETURN count(n) AS count", namespace.Knowledge))
	require.Equal(t, int64(1), neo4jCount(t, ctx, driver, "MATCH (a {kg: $kg})-[r]->(b {kg: $kg}) RETURN count(r) AS count", namespace.Knowledge))

	rollbackNamespace := types.NameSpace{
		KnowledgeBase: "g005_isolated",
		Knowledge:     namespace.Knowledge + "_rollback",
	}
	t.Cleanup(func() {
		session := driver.NewSession(context.Background(), neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite})
		defer session.Close(context.Background())
		_, cleanupErr := session.ExecuteWrite(context.Background(), func(tx neo4jdriver.ManagedTransaction) (interface{}, error) {
			_, runErr := tx.Run(context.Background(), "MATCH (n {kg: $kg}) DETACH DELETE n", map[string]interface{}{"kg": rollbackNamespace.Knowledge})
			return nil, runErr
		})
		require.NoError(t, cleanupErr)
	})
	invalidGraph := &types.GraphData{
		Relation: []*types.GraphRelation{{Node1: "Alpha", Node2: "Beta", Type: ""}},
	}
	err = repo.AddGraph(ctx, rollbackNamespace, []*types.GraphData{graph, invalidGraph})
	require.Error(t, err)
	require.Equal(t, int64(0), neo4jCount(t, ctx, driver, "MATCH (n {kg: $kg}) RETURN count(n) AS count", rollbackNamespace.Knowledge),
		"all graphs in one AddGraph call must roll back together")

	batchInvalidCases := []struct {
		name  string
		graph *types.GraphData
		want  string
	}{
		{
			name:  "empty node name",
			graph: &types.GraphData{Node: []*types.GraphNode{{Name: " "}}},
			want:  "graph node name is required",
		},
		{
			name:  "empty relationship source",
			graph: &types.GraphData{Relation: []*types.GraphRelation{{Node1: "", Node2: "Beta", Type: "RELATED_TO"}}},
			want:  "graph relationship source and target are required",
		},
		{
			name:  "empty relationship target",
			graph: &types.GraphData{Relation: []*types.GraphRelation{{Node1: "Alpha", Node2: " ", Type: "RELATED_TO"}}},
			want:  "graph relationship source and target are required",
		},
		{
			name:  "empty relationship type",
			graph: &types.GraphData{Relation: []*types.GraphRelation{{Node1: "Alpha", Node2: "Beta", Type: ""}}},
			want:  "graph relationship type is required",
		},
	}
	for i, tc := range batchInvalidCases {
		t.Run("valid prefix plus "+tc.name, func(t *testing.T) {
			batchNamespace := types.NameSpace{
				KnowledgeBase: "g005_isolated",
				Knowledge:     namespace.Knowledge + fmt.Sprintf("_batch_%d", i),
			}
			cleanupNeo4jKnowledge(t, driver, batchNamespace.Knowledge)

			err := repo.AddGraph(ctx, batchNamespace, []*types.GraphData{graph, tc.graph})
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, int64(0), neo4jCount(t, ctx, driver,
				"MATCH (n {kg: $kg}) RETURN count(n) AS count", batchNamespace.Knowledge),
				"batch validation must reject an invalid suffix before writing the valid prefix")
			require.Equal(t, int64(0), neo4jCount(t, ctx, driver,
				"MATCH (a {kg: $kg})-[r]->(b {kg: $kg}) RETURN count(r) AS count", batchNamespace.Knowledge))
		})
	}
}

func cleanupNeo4jKnowledge(t *testing.T, driver neo4jdriver.Driver, knowledgeID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite})
		defer session.Close(ctx)
		_, err := session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (interface{}, error) {
			_, runErr := tx.Run(ctx, "MATCH (n {kg: $kg}) DETACH DELETE n", map[string]interface{}{"kg": knowledgeID})
			return nil, runErr
		})
		require.NoError(t, err)
	})
}

func neo4jCount(t *testing.T, ctx context.Context, driver neo4jdriver.Driver, query, knowledgeID string) int64 {
	t.Helper()
	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)
	value, err := session.ExecuteRead(ctx, func(tx neo4jdriver.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx, query, map[string]interface{}{"kg": knowledgeID})
		if err != nil {
			return nil, err
		}
		if !result.Next(ctx) {
			return nil, result.Err()
		}
		count, _ := result.Record().Get("count")
		return count, nil
	})
	require.NoError(t, err)
	count, ok := value.(int64)
	require.True(t, ok, "Neo4j count type was %T", value)
	return count
}
