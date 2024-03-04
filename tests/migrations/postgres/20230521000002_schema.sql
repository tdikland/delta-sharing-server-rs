CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS "schema" (
    id UUID PRIMARY KEY NOT NULL DEFAULT (uuid_generate_v4()),
    name VARCHAR NOT NULL,
    share_id UUID NOT NULL REFERENCES share(id),
    UNIQUE (name, share_id)
);
