-- Adds the resource name to data_dict_registry (required by GET /v1/resources/search).
--
-- database/init.sql already creates the column for NEW databases. Run this script only
-- against databases created before that change (e.g. an existing docker-compose
-- ./data_store volume, where init.sql is not executed again):
--   psql -h localhost -U admin -d resourcesdb -f database/migrations/001_add_name_data_to_data_dict_registry.sql
--
-- Names were never persisted before this change, so existing rows cannot recover their
-- original name: they receive the placeholder 'resource_<dictionary_id>'.
-- The script is idempotent.
BEGIN;
ALTER TABLE data_dict_registry ADD COLUMN IF NOT EXISTS name_data VARCHAR(150);
UPDATE data_dict_registry SET name_data = 'resource_' || dictionary_id WHERE name_data IS NULL;
ALTER TABLE data_dict_registry ALTER COLUMN name_data SET NOT NULL;
COMMIT;
