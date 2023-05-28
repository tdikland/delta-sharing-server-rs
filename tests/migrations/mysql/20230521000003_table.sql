CREATE TABLE IF NOT EXISTS `table` (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    storage_path VARCHAR(255) NOT NULL,
    storage_format VARCHAR(255),
    schema_id VARCHAR(255) NOT NULL REFERENCES `schema` (id)
);