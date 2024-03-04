CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS "share_acl" (
    id UUID PRIMARY KEY NOT NULL DEFAULT (uuid_generate_v4()),
    client_id UUID NOT NULL REFERENCES "client"(id),
    share_id UUID NOT NULL REFERENCES "share"(id)
);

CREATE TABLE IF NOT EXISTS "schema_acl" (
    id UUID PRIMARY KEY NOT NULL DEFAULT (uuid_generate_v4()),
    client_id UUID NOT NULL REFERENCES "client"(id),
    schema_id UUID NOT NULL REFERENCES "schema"(id)
);

CREATE TABLE IF NOT EXISTS "table_acl" (
    id UUID PRIMARY KEY NOT NULL DEFAULT (uuid_generate_v4()),
    client_id UUID NOT NULL REFERENCES "client"(id),
    table_id UUID NOT NULL REFERENCES "table"(id)
);