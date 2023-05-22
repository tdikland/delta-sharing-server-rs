CREATE TABLE IF NOT EXISTS "table" (
    id UUID PRIMARY KEY,
    name VARCHAR NOT NULL,
    storage_path VARCHAR NOT NULL,
    storage_format VARCHAR,
    schema_id UUID NOT NULL REFERENCES "schema"(id)
);