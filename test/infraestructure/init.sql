\c postgres
DROP DATABASE IF EXISTS resourcesdb WITH (FORCE);
CREATE DATABASE resourcesdb WITH OWNER = admin ENCODING = 'UTF8';
\c resourcesdb

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



