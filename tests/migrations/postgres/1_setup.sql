CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS "share" (
    id UUID PRIMARY KEY NOT NULL DEFAULT (uuid_generate_v4()),
    name VARCHAR UNIQUE NOT NULL 
);

CREATE TABLE IF NOT EXISTS "schema" (
    id UUID PRIMARY KEY NOT NULL DEFAULT (uuid_generate_v4()),
    name VARCHAR NOT NULL,
    share_id UUID NOT NULL REFERENCES share(id),
    UNIQUE (name, share_id)
);

CREATE TABLE IF NOT EXISTS "table" (
    id UUID PRIMARY KEY NOT NULL DEFAULT (uuid_generate_v4()),
    schema_id UUID NOT NULL REFERENCES "schema"(id),
    name VARCHAR NOT NULL,
    storage_path VARCHAR NOT NULL,
    UNIQUE (name, schema_id)
);

CREATE TABLE IF NOT EXISTS "client" (
    id UUID PRIMARY KEY NOT NULL DEFAULT (uuid_generate_v4()),
    name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS "share_acl" (
    id UUID PRIMARY KEY NOT NULL DEFAULT (uuid_generate_v4()),
    client_id UUID NOT NULL REFERENCES "client"(id),
    share_id UUID NOT NULL REFERENCES "share"(id)
);