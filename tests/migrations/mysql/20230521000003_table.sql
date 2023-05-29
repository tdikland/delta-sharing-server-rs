CREATE TABLE `table` (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    storage_path VARCHAR(255) NOT NULL,
    storage_format VARCHAR(255),
    schema_id INT NOT NULL REFERENCES `schema` (id)
);