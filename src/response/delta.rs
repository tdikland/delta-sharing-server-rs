use std::collections::HashMap;

struct DeltaProtocol {
    min_reader_version: u32,
    min_writer_version: u32,
    reader_features: Vec<String>,
    writer_features: Vec<String>,
}

struct Protocol {
    delta_protocol: DeltaProtocol,
}

struct Format {
    provider: String,
    options: Option<HashMap<String, String>>,
}

struct DeltaMetadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: Format,
    schema_string: String,
    partition_columns: Vec<String>,
    created_time: Option<u64>,
    configuration: HashMap<String, String>,
}

struct Metadata {
    delta_metadata: DeltaMetadata,
    version: Option<u64>,
    size: Option<u64>,
    num_files: Option<u64>,
}

struct Action;

struct File {
    id: String,
    deletion_vector_file_id: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<u64>,
    delta_single_action: Action,
}
