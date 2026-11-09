-- pgvector + resource_user_access table (vector table of user accesses to resources).
-- database/init.sql already creates both for NEW databases; run this only against databases
-- created before that change (e.g. an existing ./data_store volume). Idempotent.
--
-- REQUIRES the pgvector image (docker-compose.yaml: pgvector/pgvector:pg16). Moving an existing
-- ./data_store from postgres:16-alpine (musl) to that image (Debian, glibc) changes how text is
-- sorted, so text indexes built before must be rebuilt once after the switch (outside a transaction):
--   psql -h localhost -U admin -d resourcesdb -c "REINDEX DATABASE resourcesdb"
-- Then:
--   psql -h localhost -U admin -d resourcesdb -f database/migrations/003_create_resource_user_access.sql
BEGIN;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE SEQUENCE IF NOT EXISTS resource_user_access_seq;

CREATE TABLE IF NOT EXISTS resource_user_access (
    id BIGINT NOT NULL DEFAULT nextval('resource_user_access_seq'),
    username VARCHAR(150) NOT NULL,
    resource_id INTEGER NOT NULL,
    predicate TEXT NOT NULL,
    predicate_embed vector,
    model_name VARCHAR(255),
    CONSTRAINT resource_user_access_pkey PRIMARY KEY (id),
    CONSTRAINT ck_resource_user_access_embed_with_model CHECK ((predicate_embed IS NULL) = (model_name IS NULL)),
    CONSTRAINT fk_resource_id_data_dict_registry FOREIGN KEY (resource_id) REFERENCES data_dict_registry(dictionary_id)
);
CREATE INDEX IF NOT EXISTS idx_resource_user_access_resource_id ON resource_user_access (resource_id);
COMMIT;
