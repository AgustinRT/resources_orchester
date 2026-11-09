\c postgres
DROP DATABASE IF EXISTS resourcesdb WITH (FORCE);
CREATE DATABASE resourcesdb WITH OWNER = admin ENCODING = 'UTF8';
\c resourcesdb

-- pgvector: vector data type and similarity operators (needs the pgvector/pgvector image, see docker-compose.yaml).
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TYPE data_use AS ENUM ('ASYNC_MESSAGE', 'DB_TABLE', 'REST_OBJ');

drop table IF EXISTS type_applications_definition CASCADE;
drop SEQUENCE IF EXISTS type_applications_definition_seq;
CREATE SEQUENCE type_applications_definition_seq;

CREATE TABLE type_applications_definition (
    definition_id  INTEGER NOT NULL DEFAULT nextval('type_applications_definition_seq'),
    name_definition VARCHAR(150) NOT NULL UNIQUE,
    description_application VARCHAR NOT NULL
);
ALTER TABLE type_applications_definition ADD PRIMARY KEY (definition_id);

drop table IF EXISTS registry_applications CASCADE;
drop SEQUENCE IF EXISTS registry_applications_seq;
CREATE SEQUENCE registry_applications_seq;

CREATE TABLE registry_applications (
    application_id INTEGER NOT NULL DEFAULT nextval('registry_applications_seq'),
    name_application VARCHAR(150) NOT NULL UNIQUE,
    description_application VARCHAR NOT NULL,
    type_application_id INTEGER NOT NULL,
    location_application JSONB DEFAULT '{}'::JSONB
);

ALTER TABLE registry_applications ADD PRIMARY KEY (application_id);
CREATE INDEX idx_applications_type_application_id ON registry_applications (type_application_id);
ALTER TABLE registry_applications ADD CONSTRAINT fk_type_application_id_type_applications_definition
    FOREIGN KEY (type_application_id) REFERENCES type_applications_definition(definition_id);


drop table IF EXISTS data_dict_registry CASCADE;
drop SEQUENCE IF EXISTS data_dict_registry_seq;
CREATE SEQUENCE data_dict_registry_seq;

CREATE TABLE data_dict_registry (
    dictionary_id INTEGER NOT NULL DEFAULT nextval('data_dict_registry_seq'),
    name_data VARCHAR(150) NOT NULL,
    data_use_dictionay data_use NOT NULL,
    description_data JSONB NOT NULL,
    fields JSONB DEFAULT '[]'::JSONB
);
ALTER TABLE data_dict_registry ADD PRIMARY KEY (dictionary_id);


DROP TABLE IF EXISTS link_dict_application CASCADE;
CREATE TABLE link_dict_application (
    dictionary_id INTEGER NOT NULL,
    application_id INTEGER NOT NULL
);

ALTER TABLE link_dict_application ADD PRIMARY KEY (dictionary_id, application_id);

CREATE INDEX idx_link_dict_application_dictionary_id ON link_dict_application (dictionary_id);
ALTER TABLE link_dict_application ADD CONSTRAINT fk_dictionary_id_data_dict_registry
    FOREIGN KEY (dictionary_id) REFERENCES data_dict_registry(dictionary_id);

CREATE INDEX idx_link_dict_application_application_id ON link_dict_application (application_id);
ALTER TABLE link_dict_application ADD CONSTRAINT fk_application_id_registry_applications
    FOREIGN KEY (application_id) REFERENCES registry_applications(application_id);


-- Accesses of users to resources, with the embedding of the prompt they used (vector table).
-- * username identifies ONE user, but a user has many accesses: it is NOT unique here.
-- * predicate_embed has no fixed dimension yet (it depends on the embedding model, see model_name);
--   fix it and add an HNSW index once the model is chosen. The vector and the model that built it
--   are stored together or not at all (an access is registered before its embedding exists).
-- * The FK has no ON DELETE action, like the other FKs: a resource with accesses cannot be deleted.
drop table IF EXISTS resource_user_access CASCADE;
drop SEQUENCE IF EXISTS resource_user_access_seq;
CREATE SEQUENCE resource_user_access_seq;

CREATE TABLE resource_user_access (
    id BIGINT NOT NULL DEFAULT nextval('resource_user_access_seq'),
    username VARCHAR(150) NOT NULL,
    resource_id INTEGER NOT NULL,
    predicate TEXT NOT NULL,
    predicate_embed vector,
    model_name VARCHAR(255),
    CONSTRAINT ck_resource_user_access_embed_with_model CHECK ((predicate_embed IS NULL) = (model_name IS NULL))
);
ALTER TABLE resource_user_access ADD PRIMARY KEY (id);

CREATE INDEX idx_resource_user_access_resource_id ON resource_user_access (resource_id);
ALTER TABLE resource_user_access ADD CONSTRAINT fk_resource_id_data_dict_registry
    FOREIGN KEY (resource_id) REFERENCES data_dict_registry(dictionary_id);


-- Transactional outbox: events are written in the same transaction as the data change and
-- published to Kafka by the outbox relay after the commit (at-least-once delivery).
DROP TABLE IF EXISTS event_outbox CASCADE;
CREATE TABLE event_outbox (
    event_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,          -- set when the event can never be published (dead-lettered)
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX idx_event_outbox_pending ON event_outbox (event_id) WHERE published_at IS NULL AND failed_at IS NULL;


----------------------------------------------------------------------------------------------------------------------------------

INSERT INTO type_applications_definition (name_definition, description_application)
VALUES ('MSH', 'Micro servicio hexagonal con multiples puertas de entrada/salida'),
        ('MSS', 'Micro servicio MVC, solo tiene comunicación sincrona'),
        ('DB', 'Estructura de bbdd (tabla, vista, documento, funcion, etc), con una localización concreta y permisos de acceso/edición'),
        ('APPB', 'Applicación front-end que consume los datos del back-end en un navegador'),
        ('APPN', 'Applicación nativa de un dispositivo que consume los datos del back-end');


INSERT INTO registry_applications (name_application, description_application, type_application_id, location_application)
VALUES ('MS_producer_question', 'Micro servicio con capacidad de emisión de mensages con preguntas a procesar', (select tad.definition_id from type_applications_definition tad where tad.name_definition = 'MSH'), '{"region": "eu-west-1", "group": "question_procesor", "familly": "ms_order"}'::JSONB),
        ('MS_listener_question', 'Micro servicio con capacidad de escucha a mensages a procesar con el MCP', (select tad.definition_id from type_applications_definition tad where tad.name_definition = 'MSS'), '{"region": "eu-west-1", "group": "question_procesor", "familly": "ms_procesor"}'::JSONB);



