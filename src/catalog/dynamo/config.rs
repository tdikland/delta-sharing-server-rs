const DYNAMO_ATTRIBUTE_PK: &'static str = "PK";
const DYNAMO_ATTRIBUTE_SK: &'static str = "SK";
const DYNAMO_ATTRIBUTE_SHARE_ID: &'static str = "share_id";
const DYNAMO_ATTRIBUTE_SHARE_NAME: &'static str = "share_name";
const DYNAMO_ATTRIBUTE_SCHEMA_NAME: &'static str = "schema_name";
const DYNAMO_ATTRIBUTE_TABLE_ID: &'static str = "table_id";
const DYNAMO_ATTRIBUTE_TABLE_NAME: &'static str = "table_name";
const DYNAMO_ATTRIBUTE_TABLE_STORAGE_LOCATION: &'static str = "table_storage_location";

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

    pub fn with_client_id_attr_name(mut self, client_id_attr_name: String) -> Self {
        self.client_id_attr_name = Some(client_id_attr_name);
        self
    }

    pub fn with_securable_attr_name(mut self, securable_attr_name: String) -> Self {
        self.securable_attr_name = Some(securable_attr_name);
        self
    }

    pub fn with_share_id_attr_name(mut self, share_id_attr_name: String) -> Self {
        self.share_id_attr_name = Some(share_id_attr_name);
        self
    }

    pub fn with_share_name_attr_name(mut self, share_name_attr_name: String) -> Self {
        self.share_name_attr_name = Some(share_name_attr_name);
        self
    }

    pub fn with_schema_name_attr_name(mut self, schema_name_attr_name: String) -> Self {
        self.schema_name_attr_name = Some(schema_name_attr_name);
        self
    }

    pub fn with_table_id_attr_name(mut self, table_id_attr_name: String) -> Self {
        self.table_id_attr_name = Some(table_id_attr_name);
        self
    }

    pub fn with_table_name_attr_name(mut self, table_name_attr_name: String) -> Self {
        self.table_name_attr_name = Some(table_name_attr_name);
        self
    }

    pub fn with_table_storage_location_attr_name(
        mut self,
        table_storage_location_attr_name: String,
    ) -> Self {
        self.table_stroage_location_attr_name = Some(table_storage_location_attr_name);
        self
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn client_id(&self) -> &str {
        self.client_id_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_PK)
    }

    pub fn securable(&self) -> &str {
        self.securable_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_SK)
    }

    pub fn share_id(&self) -> &str {
        self.share_id_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_SHARE_ID)
    }

    pub fn share_name(&self) -> &str {
        self.share_name_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_SHARE_NAME)
    }

    pub fn schema_name(&self) -> &str {
        self.schema_name_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_SCHEMA_NAME)
    }

    pub fn table_id(&self) -> &str {
        self.table_id_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_TABLE_ID)
    }

    pub fn table_name_attr(&self) -> &str {
        self.table_name_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_TABLE_NAME)
    }

    pub fn table_storage_location(&self) -> &str {
        self.table_stroage_location_attr_name
            .as_deref()
            .unwrap_or(DYNAMO_ATTRIBUTE_TABLE_STORAGE_LOCATION)
    }
}
