-- Mirrors versioned migration 000094_skill_install_transcript.

ALTER TABLE tenant_skills ADD COLUMN install_session_id TEXT;
ALTER TABLE tenant_skills ADD COLUMN install_message_id TEXT;
