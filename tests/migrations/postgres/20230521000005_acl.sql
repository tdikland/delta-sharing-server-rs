CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS "share_acl" (
    id UUID PRIMARY KEY NOT NULL DEFAULT (uuid_generate_v4()),
    client_id UUID NOT NULL REFERENCES "client"(id),
    share_id UUID NOT NULL REFERENCES "share"(id)
);