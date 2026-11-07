-- Transactional outbox (review Phase 3). database/init.sql already creates it for NEW databases;
-- run this only against databases created before that change. Idempotent.
--   psql -h localhost -U admin -d resourcesdb -f database/migrations/002_create_event_outbox.sql
BEGIN;
CREATE TABLE IF NOT EXISTS event_outbox (
    event_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,          -- set when the event can never be published (dead-lettered)
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_event_outbox_pending ON event_outbox (event_id) WHERE published_at IS NULL AND failed_at IS NULL;
COMMIT;
