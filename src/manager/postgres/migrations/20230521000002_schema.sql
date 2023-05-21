CREATE TABLE IF NOT EXISTS "schema" (
    id UUID PRIMARY KEY,
    name VARCHAR NOT NULL,
    share_id UUID NOT NULL REFERENCES share(id)
);

