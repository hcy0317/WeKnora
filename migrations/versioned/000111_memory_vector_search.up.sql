-- Migration 000095: move memory vector search into the database.
--
-- Memory vectors were written as a BYTEA blob and scored in the application,
-- which forced recall to pick its candidates before it knew anything about
-- the question: it listed a few hundred items by importance and only then
-- looked at similarity. Importance is uncorrelated with relevance, so every
-- memory below that cut was unreachable no matter how well it matched.
--
-- The fix is to let the database rank, which means the vector has to be a
-- vector to it. `embedding` mirrors the BYTEA column; BYTEA stays the source
-- of truth so a deployment without pgvector keeps working unchanged.
--
-- No HNSW index. An approximate index is filtered after the fact, so a scan
-- restricted to one subject would get its k nearest neighbours from the whole
-- table and then throw nearly all of them away. One subject holds at most a
-- couple of thousand rows, and the scope index below makes exact ordering over
-- that a few milliseconds — correct and fast beats approximate and wrong here.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'memory_item_embeddings'
    ) THEN
        RAISE NOTICE '[Migration 000095] memory_item_embeddings absent · skipping';
        RETURN;
    END IF;

    -- Deployments whose RETRIEVE_DRIVER excludes postgres run 000002 with
    -- app.skip_embedding=true and may not create the vector extension. Some
    -- externally managed databases also install vector without its halfvec
    -- type; keep the in-process fallback available in both cases.
    IF to_regtype('halfvec') IS NULL THEN
        RAISE NOTICE '[Migration 000095] halfvec type absent · memory keeps scoring in process';
    ELSE
        ALTER TABLE memory_item_embeddings ADD COLUMN IF NOT EXISTS embedding halfvec;
        RAISE NOTICE '[Migration 000095] memory_item_embeddings.embedding ready';
    END IF;

    -- Useful with or without pgvector: both the SQL ranking and the fallback
    -- read every vector of one (subject, model, dims), and nothing else.
    CREATE INDEX IF NOT EXISTS idx_mem_emb_search
        ON memory_item_embeddings (tenant_id, subject_id, model_id, dims);
END $$;
