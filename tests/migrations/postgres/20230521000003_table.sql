CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS "table" (
    id UUID PRIMARY KEY NOT NULL DEFAULT (uuid_generate_v4()),
    schema_id UUID NOT NULL REFERENCES "schema"(id),
    name VARCHAR NOT NULL,
    storage_path VARCHAR NOT NULL
);