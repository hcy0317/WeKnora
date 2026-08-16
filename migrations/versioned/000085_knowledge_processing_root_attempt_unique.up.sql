CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_processing_spans_root_attempt_unique
    ON knowledge_processing_spans (knowledge_id, attempt)
    WHERE kind = 'root';
