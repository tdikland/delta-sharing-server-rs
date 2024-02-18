//! A TableManager implementation leveraging AWS DynamoDB.

use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use aws_sdk_dynamodb::{
    operation::query::{builders::QueryFluentBuilder, QueryOutput},
    types::AttributeValue,
    Client,
};
use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};

use crate::protocol::securable::{Schema, SchemaBuilder, Share, ShareBuilder, Table, TableBuilder};

use super::{Catalog, CatalogError, List, ListCursor};

/// ASSUMPTION: the writer only uses transactions to guarantee the consistency of the
/// shared securables (i.e. share -> schema -> table).
///
/// | SHARE#ALL | share1 | ...
/// | SHARE#ALL | share2 | ...
/// | SHARE#share1 | schema1 | ...
/// | SHARE#share1 | schema2 | ...
/// | SHARE#share2 | schema1#table1 | ...

/// TableManager using AWS DynamoDB to store shared objects.
///
/// ## Table layout
///
/// | PK | SK | share_id | storage_path | table_id
/// SHARE#{share_name}#SCHEMA#ALL#TABLE#ALL | SHARE | share1_id
/// SHARE#{share_name}#SCHEMA#{schema_name}#TABLE#ALL | SCHEMA |
/// SHARE#{share_name}#SCHEMA#{schema_name}#TABLE#{table_name} | TABLE | share1_id | s3://my-data-bucket/my-table-root/ | table1_id
///
///  Key
/// 1. KEY: PK+SK
/// 2. GSI: SK+PK
///
/// Implemented query patterns
/// 1. QUERY on GSI with SK = SHARE
/// 2. GET on KEY with PK = SHARE#{share_name}#SCHEMA#ALL#TABLE#ALL
/// 3. QUERY on GSI with SK = SCHEMA AND PK begins_with(SHARE#{share_name})
/// 4. QUERY on GSI with type = TABLE and SK begins_with(SHARE#{share_name}#SCHEMA#{schema_name})
/// 5. QUERY on GSI with type = TABLE and SK begins_with(SHARE#{share_name})
/// 6. GET on KEY with PK = SHARE#{share_name}#SCHEMA#{schema_name}#TABLE#{table_name} AND SK = TABLE
///
/// ## Query patterns
/// 1. Get a table by share_name, schema_name and table_name
/// 2. Get a share by share_name
/// 2. List all shares
/// 3. List all schemas in a share
/// 4. List all tables in a share
/// 5. List all tables in a schema
///
/// 
/// | client_id | securable |
/// | anonymous | SHARE#share1 | ...
/// | anonymous | SCHEMA#share1.schema1 | ..
/// | anonymous | TABLE#share1.schema1.table1 | ...
/// 
/// 1. list shares for client
/// 2. list schemas for share for client
/// 3. list tables for share for client
/// 4. list tables for schema for client
/// 5. get share for client
/// 6. get table for client
/// 
/// 1. QUERY on PK = client_id AND SK begins_with(SHARE)
/// 2. QUERY on PK = client_id AND SK begins_with(SCHEMA#share_name)
/// 3. QUERY on PK = client_id AND SK begins_with(TABLE#share_name)
/// 4. QUERY on PK = client_id AND SK begins_with(TABLE#share_name.schema_name)
/// 5. GET on PK = client_id AND SK = SHARE#share_name
/// 6. GET on PK = client_id AND SK = TABLE#share_name.schema_name.table_name 
#[derive(Debug)]
pub struct DynamoShareReader {
    client: Client,
    table_name: String,
    index_name: String,
}

impl DynamoShareReader {
    /// Create a new TableManager using the AWS DynamoDB client along with
    /// table_name and GSI index name.
    pub fn new(client: Client, table_name: String, index_name: String) -> Self {
        Self {
            client,
            table_name,
            index_name,
        }
    }

    /// Create a new DynamoDB table to store shares, schemas and tables.
    pub fn create_table(&self) -> Result<(), DynamoError> {
        todo!()
    }

    /// Retrieve underlying DynamoDB SDK client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Add a new share to the share store.
    pub async fn put_share(&self, share: Share) -> Result<Share, DynamoError> {
        let key = DynamoKey::from_share_name(share.name());
        let mut req = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", key.partition_key())
            .item("SK", key.sort_key());

        if let Some(id) = share.id() {
            req = req.item("share_id", AttributeValue::S(id.to_owned()))
        }

        req.send().await.map_err(|e| DynamoError::ServiceError {
            reason: e.to_string(),
        })?;

        Ok(share)
    }

    /// Retrieve a share from the share store.
    pub async fn get_share(&self, share_name: &str) -> Result<Share, DynamoError> {
        let key = DynamoKey::from_share_name(share_name);
        self.get_securable(key).await.map_err(|e| match e {
            DynamoError::SecurableNotFound => DynamoError::ShareNotFound {
                share: share_name.to_string(),
            },
            e => e,
        })
    }

    /// Retrieve a list of shares from the share store.
    pub async fn query_shares(&self, cursor: &ListCursor) -> Result<List<Share>, DynamoError> {
        let sk = "SHARE".to_owned();
        let pk_prefix = "SHARE#".to_string();
        self.query_securable(cursor, sk, pk_prefix).await
    }

    /// Add a new schema to the share store.
    pub async fn put_schema(&self, schema: Schema) -> Result<Schema, DynamoError> {
        let key = DynamoKey::from_schema_name(schema.share_name(), schema.name());
        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", key.partition_key())
            .item("SK", key.sort_key())
            .send()
            .await
            .map_err(|e| DynamoError::ServiceError {
                reason: e.to_string(),
            })?;

        Ok(schema)
    }

    /// Retrieve a schema from the share store.
    pub async fn get_schema(
        &self,
        share_name: &str,
        schema_name: &str,
    ) -> Result<Schema, DynamoError> {
        let key = DynamoKey::from_schema_name(share_name, schema_name);
        self.get_securable(key).await
    }

    /// Retrieve a list of schemas from the share store.
    pub async fn query_schemas(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Schema>, DynamoError> {
        let sk = "SCHEMA".to_owned();
        let pk_prefix = format!("SHARE#{}", share_name);
        self.query_securable(cursor, sk, pk_prefix).await
    }

    /// Add a new table to the share store.
    pub async fn put_table(&self, table: Table) -> Result<Table, DynamoError> {
        let key = DynamoKey::from_table_name(table.share_name(), table.schema_name(), table.name());
        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", key.partition_key())
            .item("SK", key.sort_key())
            .item(
                "storage_path",
                AttributeValue::S(table.storage_path().to_owned()),
            )
            .send()
            .await
            .map_err(|e| DynamoError::ServiceError {
                reason: e.to_string(),
            })?;

        Ok(table)
    }

    /// Retrieve a table from the share store.
    pub async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, DynamoError> {
        let key = DynamoKey::from_table_name(share_name, schema_name, table_name);
        self.get_securable(key).await
    }

    /// Retrieve a list of tables from the share store.
    pub async fn query_tables_in_share(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Table>, DynamoError> {
        let sk = "TABLE".to_owned();
        let pk_prefix = format!("SHARE#{}", share_name);
        self.query_securable(cursor, sk, pk_prefix).await
    }

    /// Retrieve a list of tables from the share store.
    pub async fn query_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Table>, DynamoError> {
        let sk = "TABLE".to_owned();
        let pk_prefix = format!("SHARE#{}#SCHEMA#{}", share_name, schema_name);
        self.query_securable(cursor, sk, pk_prefix).await
    }

    async fn get_securable<
        T: for<'a> TryFrom<&'a HashMap<String, AttributeValue>, Error = DynamoError>,
    >(
        &self,
        key: DynamoKey,
    ) -> Result<T, DynamoError> {
        let get_item_output = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", key.partition_key())
            .key("SK", key.sort_key())
            .send()
            .await
            .map_err(|e| DynamoError::ServiceError {
                reason: e.to_string(),
            })?;

        let securable = get_item_output
            .item()
            .ok_or(DynamoError::SecurableNotFound)
            .and_then(TryInto::try_into)?;

        Ok(securable)
    }

    async fn query_securable<T>(
        &self,
        cursor: &ListCursor,
        sk: String,
        pk_begins_with: String,
    ) -> Result<List<T>, DynamoError>
    where
        T: for<'a> TryFrom<&'a HashMap<String, AttributeValue>, Error = DynamoError>,
    {
        let mut query = self
            .client
            .query()
            .table_name(&self.table_name)
            .index_name(&self.index_name)
            .expression_attribute_names("#SK", "SK")
            .expression_attribute_names("#PK", "PK")
            .expression_attribute_values(":sk", AttributeValue::S(sk))
            .expression_attribute_values(":pk", AttributeValue::S(pk_begins_with))
            .key_condition_expression("#SK = :sk AND begins_with(#PK, :pk)");
        query = with_cursor(query, cursor)?;

        let query_output = query.send().await;
        dbg!(&query_output);
        let query_output = query_output.map_err(|e| DynamoError::ServiceError {
            reason: e.to_string(),
        })?;
        let list_result = parse_query_output(query_output)?;
        Ok(list_result)
    }
}

fn with_cursor(
    mut query: QueryFluentBuilder,
    cursor: &ListCursor,
) -> Result<QueryFluentBuilder, DynamoError> {
    if let Some(limit) = cursor.max_results() {
        query = query.limit(limit as i32);
    }
    if let Some(token) = cursor.page_token() {
        let cursor: DynamoCursor = token.try_into()?;
        query = query.set_exclusive_start_key(Some(cursor.into_start_key()));
    }
    Ok(query)
}

fn parse_query_output<T>(output: QueryOutput) -> Result<List<T>, DynamoError>
where
    T: for<'a> TryFrom<&'a HashMap<String, AttributeValue>, Error = DynamoError>,
{
    if let Some(items) = output.items() {
        let securables = items
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<T>, _>>()?;
        let token = output
            .last_evaluated_key()
            .map(|key| DynamoCursor::try_from(key).and_then(|c| c.into_token()))
            .transpose()?;
        Ok(List::new(securables, token))
    } else {
        Ok(List::new(vec![], None))
    }
}

/// Errors that can occur when interacting with the DynamoDB share store.
#[derive(Debug)]
pub enum DynamoError {
    /// The ListCursor could not be interpreted as a DynamoCursor.
    InvalidListCursor,
    /// The DynamoCursor could not be interpreted as a ListCursor.
    InvalidDynamoCursor,
    /// The requested securable was not found.
    SecurableNotFound,
    /// The requested share was not found.
    ShareNotFound {
        /// The name of the share that was not found.
        share: String,
    },
    /// The requested schema was not found.
    SchemaNotFound {
        /// The name of the share that was searched.
        share: String,
        /// The name of the schema that was not found.
        schema: String,
    },
    /// The requested table was not found.
    TableNotFound {
        /// The name of the share that was searched.
        share: String,
        /// The name of the schema that was searched.
        schema: String,
        /// The name of the table that was not found.
        table: String,
    },
    /// The requested share could not be parsed.
    InvalidShareItem,
    /// The requested schema could not be parsed.
    InvalidSchemaItem,
    /// The requested table could not be parsed.
    InvalidTableItem,
    /// An error occurred when interacting with the DynamoDB service.
    ServiceError {
        /// The reason for the error.
        reason: String,
    },
    /// An unexpected error occurred.
    Other,
}

enum Securable {
    Share,
    Schema,
    Table,
}

struct DynamoKey {
    share_name: String,
    schema_name: Option<String>,
    table_name: Option<String>,
    securable: Securable,
}

impl DynamoKey {
    fn from_share_name(share_name: impl Into<String>) -> Self {
        Self {
            share_name: share_name.into(),
            schema_name: None,
            table_name: None,
            securable: Securable::Share,
        }
    }

    fn from_schema_name(share_name: impl Into<String>, schema_name: impl Into<String>) -> Self {
        Self {
            share_name: share_name.into(),
            schema_name: Some(schema_name.into()),
            table_name: None,
            securable: Securable::Schema,
        }
    }

    fn from_table_name(
        share_name: impl Into<String>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            share_name: share_name.into(),
            schema_name: Some(schema_name.into()),
            table_name: Some(table_name.into()),
            securable: Securable::Table,
        }
    }

    fn partition_key(&self) -> AttributeValue {
        let schema_name = self.schema_name.clone().unwrap_or("ALL".to_owned());
        let table_name = self.table_name.clone().unwrap_or("ALL".to_owned());
        let pk = format!(
            "SHARE#{}#SCHEMA#{}#TABLE#{}",
            self.share_name, schema_name, table_name
        );
        AttributeValue::S(pk)
    }

    fn sort_key(&self) -> AttributeValue {
        match self.securable {
            Securable::Share => AttributeValue::S("SHARE".to_owned()),
            Securable::Schema => AttributeValue::S("SCHEMA".to_owned()),
            Securable::Table => AttributeValue::S("TABLE".to_owned()),
        }
    }

    fn share_name(&self) -> &str {
        self.share_name.as_ref()
    }

    fn schema_name(&self) -> Option<&String> {
        self.schema_name.as_ref()
    }

    fn table_name(&self) -> Option<&String> {
        self.table_name.as_ref()
    }
}

impl Display for DynamoKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.schema_name, &self.table_name) {
            (None, None) => write!(f, "{}", self.share_name),
            (None, Some(_)) => Err(std::fmt::Error),
            (Some(schema_name), None) => write!(f, "{}.{}", self.share_name, schema_name),
            (Some(schema_name), Some(table_name)) => {
                write!(f, "{}.{}.{}", self.share_name, schema_name, table_name)
            }
        }
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for DynamoKey {
    type Error = DynamoError;

    fn try_from(item: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let pk_parts = item
            .get("PK")
            .ok_or(DynamoError::InvalidShareItem)?
            .as_s()
            .map_err(|_| DynamoError::InvalidShareItem)?
            .split('#')
            .collect::<Vec<_>>();

        // Primary key validation
        // TODO: make this better!
        if pk_parts.len() != 6 {
            return Err(DynamoError::InvalidShareItem);
        }
        let share_name = pk_parts[1].to_owned();
        let schema_name = pk_parts[3].to_owned();
        let table_name = pk_parts[5].to_owned();
        let entity = item
            .get("SK")
            .ok_or(DynamoError::InvalidShareItem)?
            .as_s()
            .map_err(|_| DynamoError::InvalidShareItem)?
            .to_owned();

        let securable = match entity.as_str() {
            "SHARE" => Securable::Share,
            "SCHEMA" => Securable::Schema,
            "TABLE" => Securable::Table,
            _ => {
                // TODO custom error message
                return Err(DynamoError::InvalidShareItem);
            }
        };

        Ok(Self {
            share_name,
            schema_name: Some(schema_name),
            table_name: Some(table_name),
            securable,
        })
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for Share {
    type Error = DynamoError;

    fn try_from(item: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let key = DynamoKey::try_from(item)?;
        let share_id = item.get("share_id").and_then(|v| v.as_s().ok().cloned());
        let share = ShareBuilder::new(key.share_name()).set_id(share_id).build();

        Ok(share)
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for Schema {
    type Error = DynamoError;

    fn try_from(item: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let key = DynamoKey::try_from(item)?;
        let share_id = item.get("share_id").and_then(|v| v.as_s().ok().cloned());
        let share = ShareBuilder::new(key.share_name()).set_id(share_id).build();

        let schema_name = key.schema_name().ok_or(DynamoError::InvalidSchemaItem)?;
        let schema = SchemaBuilder::new(share, schema_name).build();

        Ok(schema)
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for Table {
    type Error = DynamoError;

    fn try_from(item: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let key = DynamoKey::try_from(item)?;

        // required property
        let storage_path = item
            .get("storage_path")
            .ok_or(Self::Error::Other)?
            .as_s()
            .map_err(|_| Self::Error::Other)?;

        // optional properties
        let table_id = item.get("table_id").and_then(|v| v.as_s().ok().cloned());
        let table_format = item
            .get("table_format")
            .and_then(|v| v.as_s().ok().cloned());

        let share_id = item.get("share_id").and_then(|v| v.as_s().ok().cloned());
        let share = ShareBuilder::new(key.share_name()).set_id(share_id).build();

        let schema_name = key.schema_name().ok_or(DynamoError::InvalidSchemaItem)?;
        let schema = SchemaBuilder::new(share, schema_name).build();

        let table_name = key.table_name().ok_or(DynamoError::InvalidTableItem)?;
        let table = TableBuilder::new(schema, table_name, storage_path)
            .set_id(table_id)
            .set_format(table_format)
            .build();

        Ok(table)
    }
}

#[derive(Serialize, Deserialize)]
struct DynamoCursor {
    pk: String,
    sk: String,
}

impl DynamoCursor {
    fn into_token(self) -> Result<String, DynamoError> {
        let value = serde_json::to_vec(&self).map_err(|_| DynamoError::InvalidDynamoCursor)?;
        let encoded_token = general_purpose::URL_SAFE.encode(value);
        Ok(encoded_token)
    }

    fn into_start_key(self) -> HashMap<String, AttributeValue> {
        let mut start_key = HashMap::new();
        start_key.insert(String::from("PK"), AttributeValue::S(self.pk));
        start_key.insert(String::from("SK"), AttributeValue::S(self.sk));
        start_key
    }
}

impl TryFrom<&str> for DynamoCursor {
    type Error = DynamoError;

    fn try_from(token: &str) -> Result<Self, Self::Error> {
        let decoded_token = general_purpose::URL_SAFE
            .decode(token)
            .map_err(|_| DynamoError::InvalidListCursor)?;
        let cursor =
            serde_json::from_slice(&decoded_token).map_err(|_| DynamoError::InvalidListCursor)?;
        Ok(cursor)
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for DynamoCursor {
    type Error = DynamoError;

    fn try_from(value: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let pk = value
            .get("PK")
            .ok_or(DynamoError::Other)?
            .as_s()
            .map_err(|_| DynamoError::Other)?;
        let sk = value
            .get("SK")
            .ok_or(DynamoError::Other)?
            .as_s()
            .map_err(|_| DynamoError::Other)?;

        Ok(Self {
            pk: pk.to_owned(),
            sk: sk.to_owned(),
        })
    }
}

impl From<DynamoError> for CatalogError {
    fn from(value: DynamoError) -> Self {
        println!("ENCOUNTERED ERROR!: {:?}", &value);
        match value {
            DynamoError::InvalidListCursor => CatalogError::MalformedContinuationToken,
            DynamoError::ShareNotFound { share } => {
                CatalogError::ShareNotFound { share_name: share }
            }
            _ => CatalogError::Other {
                reason: String::from(""),
            },
        }
    }
}

#[async_trait]
impl Catalog for DynamoShareReader {
    async fn list_shares(&self, pagination: &ListCursor) -> Result<List<Share>, CatalogError> {
        self.query_shares(pagination).await.map_err(From::from)
    }

    async fn get_share(&self, share_name: &str) -> Result<Share, CatalogError> {
        self.get_share(share_name).await.map_err(From::from)
    }

    async fn list_schemas(
        &self,
        share_name: &str,
        pagination: &ListCursor,
    ) -> Result<List<Schema>, CatalogError> {
        self.query_schemas(share_name, pagination)
            .await
            .map_err(From::from)
    }

    async fn list_tables_in_share(
        &self,
        share_name: &str,
        pagination: &ListCursor,
    ) -> Result<List<Table>, CatalogError> {
        self.query_tables_in_share(share_name, pagination)
            .await
            .map_err(From::from)
    }

    async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        pagination: &ListCursor,
    ) -> Result<List<Table>, CatalogError> {
        self.query_tables_in_schema(share_name, schema_name, pagination)
            .await
            .map_err(From::from)
    }

    async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, CatalogError> {
        self.get_table(share_name, schema_name, table_name)
            .await
            .map_err(From::from)
    }
}
