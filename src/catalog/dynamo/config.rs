const DYNAMO_ATTRIBUTE_PK: &str = "PK";
const DYNAMO_ATTRIBUTE_SK: &str = "SK";
const DYNAMO_ATTRIBUTE_SHARE_ID: &str = "share_id";
const DYNAMO_ATTRIBUTE_SHARE_NAME: &str = "share_name";
const DYNAMO_ATTRIBUTE_SCHEMA_NAME: &str = "schema_name";
const DYNAMO_ATTRIBUTE_TABLE_ID: &str = "table_id";
const DYNAMO_ATTRIBUTE_TABLE_NAME: &str = "table_name";
const DYNAMO_ATTRIBUTE_TABLE_STORAGE_LOCATION: &str = "table_storage_location";

/// Configuration for the Dynamo catalog.
pub struct DynamoCatalogConfig {
    table_name: String,
    client_id_attr_name: Option<String>,
    securable_attr_name: Option<String>,
    share_id_attr_name: Option<String>,
    share_name_attr_name: Option<String>,
    schema_name_attr_name: Option<String>,
    table_id_attr_name: Option<String>,
    table_name_attr_name: Option<String>,
    table_stroage_location_attr_name: Option<String>,
}

impl DynamoCatalogConfig {
    /// Create a new Dynamo catalog configuration.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            client_id_attr_name: None,
            securable_attr_name: None,
            share_id_attr_name: None,
            share_name_attr_name: None,
            schema_name_attr_name: None,
            table_id_attr_name: None,
            table_name_attr_name: None,
            table_stroage_location_attr_name: None,
        }
    }

    /// Set the name of the attribute that represents the client ID.
    pub fn with_client_id_attr_name(mut self, client_id_attr_name: String) -> Self {
        self.client_id_attr_name = Some(client_id_attr_name);
        self
    }

    /// Set the name of the attribute that represents the securable.
    pub fn with_securable_attr_name(mut self, securable_attr_name: String) -> Self {
        self.securable_attr_name = Some(securable_attr_name);
        self
    }

    /// Set the name of the attribute that represents the share ID.
    pub fn with_share_id_attr_name(mut self, share_id_attr_name: String) -> Self {
        self.share_id_attr_name = Some(share_id_attr_name);
        self
    }

    /// Set the name of the attribute that represents the share name.
    pub fn with_share_name_attr_name(mut self, share_name_attr_name: String) -> Self {
        self.share_name_attr_name = Some(share_name_attr_name);
        self
    }

    /// Set the name of the attribute that represents the schema name.
    pub fn with_schema_name_attr_name(mut self, schema_name_attr_name: String) -> Self {
        self.schema_name_attr_name = Some(schema_name_attr_name);
        self
    }

    /// Set the name of the attribute that represents the table ID.
    pub fn with_table_id_attr_name(mut self, table_id_attr_name: String) -> Self {
        self.table_id_attr_name = Some(table_id_attr_name);
        self
    }

    /// Set the name of the attribute that represents the table name.
    pub fn with_table_name_attr_name(mut self, table_name_attr_name: String) -> Self {
        self.table_name_attr_name = Some(table_name_attr_name);
        self
    }

    /// Set the name of the attribute that represents the table storage location.
    pub fn with_table_storage_location_attr_name(
        mut self,
        table_storage_location_attr_name: String,
    ) -> Self {
        self.table_stroage_location_attr_name = Some(table_storage_location_attr_name);
        self
    }

    /// Get the name of the table.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get the name of the attribute that represents the client ID.
    pub fn client_id(&self) -> &str {
        self.client_id_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_PK)
    }

    /// Get the name of the attribute that represents the securable.
    pub fn securable(&self) -> &str {
        self.securable_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_SK)
    }

    /// Get the name of the attribute that represents the share ID.
    pub fn share_id(&self) -> &str {
        self.share_id_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_SHARE_ID)
    }

    /// Get the name of the attribute that represents the share name.
    pub fn share_name(&self) -> &str {
        self.share_name_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_SHARE_NAME)
    }

    /// Get the name of the attribute that represents the schema name.
    pub fn schema_name(&self) -> &str {
        self.schema_name_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_SCHEMA_NAME)
    }

    /// Get the name of the attribute that represents the table ID.
    pub fn table_id(&self) -> &str {
        self.table_id_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_TABLE_ID)
    }

    /// Get the name of the attribute that represents the table name.
    pub fn table_name_attr(&self) -> &str {
        self.table_name_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_TABLE_NAME)
    }

    /// Get the name of the attribute that represents the table storage location.
    pub fn table_storage_location(&self) -> &str {
        self.table_stroage_location_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_TABLE_STORAGE_LOCATION)
    }
}
