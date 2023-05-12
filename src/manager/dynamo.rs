//! A TableManager implementation leveraging AWS DynamoDB.
//!
//! ## DynamoDB table layout
//!
//! | PK | SK | share_id | storage_path | table_id
//! SHARE#{share_name}#SCHEMA#ALL#TABLE#ALL | SHARE | share1_id
//! SHARE#{share_name}#SCHEMA#{schema_name}#TABLE#ALL | SCHEMA |
//! SHARE#{share_name}#SCHEMA#{schema_name}#TABLE#{table_name} | TABLE | share1_id | s3://!my-data-bucket/my-table-root/ | table1_id
//!
//! Key
//! 1. KEY: PK+SK
//! 2. GSI: SK+PK
//!
//! Implemented query patterns
//! 1. QUERY on GSI with SK = SHARE
//! 2. GET on KEY with PK = SHARE#{share_name}#SCHEMA#ALL#TABLE#ALL
//! 3. QUERY on GSI with SK = SCHEMA AND PK begins_with(SHARE#{share_name})
//! 4. QUERY on GSI with type = TABLE and SK begins_with(SHARE#{share_name}#SCHEMA#{schema_name})
//! 5. QUERY on GSI with type = TABLE and SK begins_with(SHARE#{share_name})
//! 6. GET on KEY with PK = SHARE#{share_name}#SCHEMA#{schema_name}#TABLE#{table_name} AND SK = TABLE

use std::collections::HashMap;

use async_trait::async_trait;
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};

use crate::protocol::shared::{Schema, Share, Table};

use super::{List, ListCursor, TableManager, TableManagerError};

// TABLE LAYOUT
// ------------
//
// | PK | SK | share_id | storage_path | table_id
// SHARE#{share_name}#SCHEMA#ALL#TABLE#ALL | SHARE | share1_id
// SHARE#{share_name}#SCHEMA#{schema_name}#TABLE#ALL | SCHEMA |
// SHARE#{share_name}#SCHEMA#{schema_name}#TABLE#{table_name} | TABLE | share1_id | s3://my-data-bucket/my-table-root/ | table1_id

// Key
// 1. KEY: PK+SK
// 2. GSI: SK+PK
//
// Implemented query patterns
// 1. QUERY on GSI with SK = SHARE
// 2. GET on KEY with PK = SHARE#{share_name}#SCHEMA#ALL#TABLE#ALL
// 3. QUERY on GSI with SK = SCHEMA AND PK begins_with(SHARE#{share_name})
// 4. QUERY on GSI with type = TABLE and SK begins_with(SHARE#{share_name}#SCHEMA#{schema_name})
// 5. QUERY on GSI with type = TABLE and SK begins_with(SHARE#{share_name})
// 6. GET on KEY with PK = SHARE#{share_name}#SCHEMA#{schema_name}#TABLE#{table_name} AND SK = TABLE

pub struct DynamoTableManager {
    client: Client,
    config: DynamoConfig,
}

impl DynamoTableManager {
    pub fn new(client: Client, config: DynamoConfig) -> Self {
        Self { client, config }
    }

    pub async fn put_share(&self, share: &Share) -> Result<(), DynamoError> {
        let pk = format!("SHARE#{}#SCHEMA#ALL#TABLE#ALL", share.name());
        let sk = String::from("SHARE");
        let mut req = self
            .client
            .put_item()
            .table_name(self.config.table_name())
            .item("PK", AttributeValue::S(pk))
            .item("SK", AttributeValue::S(sk));

        if let Some(id) = share.id() {
            req = req.item("share_id", AttributeValue::S(id.to_owned()))
        }

        req.send().await.map_err(|e| DynamoError::ServiceError {
            reason: e.to_string(),
        })?;

        Ok(())
    }

    pub async fn get_share(&self, share_name: &str) -> Result<Share, DynamoError> {
        let pk = format!("SHARE#{}#SCHEMA#ALL#TABLE#ALL", share_name);
        let sk = String::from("SHARE");
        let get_item_output = self
            .client
            .get_item()
            .table_name(self.config.table_name())
            .key("PK", AttributeValue::S(pk))
            .key("SK", AttributeValue::S(sk))
            .send()
            .await
            .unwrap();

        let share = get_item_output
            .item()
            .ok_or(DynamoError::ShareNotFound {
                share: share_name.to_owned(),
            })
            .and_then(DynamoShareItem::try_from)?
            .into_inner();

        Ok(share)
    }

    pub async fn query_shares(&self, pagination: &ListCursor) -> Result<List<Share>, DynamoError> {
        let mut query = self
            .client
            .query()
            .table_name(self.config.table_name())
            .index_name(self.config.index_name())
            .expression_attribute_names("#SK", "SK")
            .expression_attribute_values(":sk", AttributeValue::S("SHARE".to_owned()))
            .key_condition_expression("#SK = :sk");

        if let Some(limit) = pagination.max_results() {
            query = query.limit(limit as i32);
        }
        if let Some(token) = pagination.page_token() {
            let cursor: DynamoCursor = token.try_into()?;
            query = query.set_exclusive_start_key(Some(cursor.into_start_key()));
        }

        let query_output = query.send().await.unwrap();

        if let Some(items) = query_output.items() {
            let shares = items
                .iter()
                .map(|item| {
                    DynamoShareItem::try_from(item)
                        .and_then(|share_item| Ok(share_item.into_inner()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let token = query_output
                .last_evaluated_key()
                .map(|key| DynamoCursor::try_from(key).and_then(|c| c.into_token()))
                .transpose()?;

            Ok(List::new(shares, token))
        } else {
            Ok(List::new(vec![], None))
        }
    }

    pub async fn put_schema(&self, share_name: &str, schema_name: &str) -> Result<(), ()> {
        let pk = format!("SHARE#{}#SCHEMA#{}#TABLE#ALL", share_name, schema_name);
        let sk = format!("SCHEMA");
        self.client
            .put_item()
            .table_name(self.config.table_name.clone())
            .item("PK", AttributeValue::S(pk))
            .item("SK", AttributeValue::S(sk))
            .send()
            .await
            .unwrap();

        Ok(())
    }

    pub async fn get_schema(&self, share_name: &str, schema_name: &str) -> Result<(), ()> {
        // let pk = format!("SHARE#{}#SCHEMA#{}#TABLE#ALL", share_name, schema_name);
        // let sk = format!("SCHEMA");
        // self.client
        //     .put_item()
        //     .table_name(self.config.table_name.clone())
        //     .item("PK", AttributeValue::S(pk))
        //     .item("SK", AttributeValue::S(sk))
        //     .send()
        //     .await
        //     .unwrap();

        // Ok(())
        todo!()
    }

    pub async fn query_schemas(
        &self,
        share_name: &str,
        pagination: &ListCursor,
    ) -> Result<List<Schema>, TableManagerError> {
        let pk = format!("SHARE#{}", share_name);
        let mut query = self
            .client
            .query()
            .table_name(self.config.table_name.clone())
            .index_name(self.config.index_name.clone())
            .expression_attribute_names("#PK", "PK")
            .expression_attribute_names("#SK", "SK")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":sk", AttributeValue::S("SCHEMA".to_owned()))
            .key_condition_expression("#SK = :sk AND begins_with(#PK, :pk)");

        // Handle pagination requirements and set cursor to correct position in collection
        if let Some(limit) = pagination.max_results() {
            query = query.limit(limit as i32);
        }
        if let Some(cursor) = pagination.to_cursor::<DynamoCursor>().unwrap() {
            query = query.set_exclusive_start_key(Some(cursor.into_start_key()));
        }

        // Fire DynamoDB query
        let query_output = query.send().await.unwrap();

        // Encode cursor position into pagination token
        let token = query_output.last_evaluated_key().and_then(|k| {
            let cursor = DynamoCursor::try_from_last_key(k).unwrap();
            let token = ListCursor::from_cursor(&cursor).unwrap();
            Some(token)
        });

        // Parse output
        if let Some(items) = query_output.items() {
            let schemas = items
                .iter()
                .map(|i| DynamoItem::try_from(i).unwrap().try_into_schema().unwrap())
                .collect::<Vec<_>>();
            Ok(List::new(schemas, token))
        } else {
            Ok(List::new(vec![], token))
        }
    }

    pub async fn put_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        storage_path: &str,
    ) -> Result<(), ()> {
        let pk = format!(
            "SHARE#{}#SCHEMA#{}#TABLE#{}",
            share_name, schema_name, table_name
        );
        let sk = format!("TABLE");
        self.client
            .put_item()
            .table_name(self.config.table_name.clone())
            .item("PK", AttributeValue::S(pk))
            .item("SK", AttributeValue::S(sk))
            .item("storage_path", AttributeValue::S(storage_path.to_owned()))
            .send()
            .await
            .unwrap();

        Ok(())
    }

    pub async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, TableManagerError> {
        let pk = format!(
            "SHARE#{}#SCHEMA#{}#TABLE#{}",
            share_name, schema_name, table_name
        );
        let get_item_output = self
            .client
            .get_item()
            .table_name(self.config.table_name.clone())
            .key("PK", AttributeValue::S(pk))
            .key("SK", AttributeValue::S("TABLE".to_owned()))
            .send()
            .await
            .unwrap();

        if let Some(item) = get_item_output.item() {
            let table = DynamoItem::try_from(item)
                .unwrap()
                .try_into_table()
                .unwrap();
            Ok(table)
        } else {
            Err(TableManagerError::TableNotFound {
                share_name: share_name.to_owned(),
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
            })
        }
    }

    pub async fn query_tables_in_share(
        &self,
        share_name: &str,
        pagination: &ListCursor,
    ) -> Result<List<Table>, TableManagerError> {
        let pk = format!("SHARE#{}", share_name);
        let mut query = self
            .client
            .query()
            .table_name(self.config.table_name.clone())
            .index_name(self.config.index_name.clone())
            .expression_attribute_names("#SK", "SK")
            .expression_attribute_names("#PK", "PK")
            .expression_attribute_values(":sk", AttributeValue::S("TABLE".to_owned()))
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .key_condition_expression("#SK = :sk AND begins_with(#PK, :pk)");

        // Handle pagination requirements and set cursor to correct position in collection
        if let Some(limit) = pagination.max_results() {
            query = query.limit(limit as i32);
        }
        if let Some(cursor) = pagination.to_cursor::<DynamoCursor>().unwrap() {
            query = query.set_exclusive_start_key(Some(cursor.into_start_key()));
        }

        // Fire DynamoDB query
        let query_output = query.send().await.unwrap();

        // Encode cursor position into pagination token
        let token = query_output.last_evaluated_key().and_then(|k| {
            let cursor = DynamoCursor::try_from_last_key(k).unwrap();
            let token = ListCursor::from_cursor(&cursor).unwrap();
            Some(token)
        });

        if let Some(items) = query_output.items() {
            let tables = items
                .iter()
                .map(|i| DynamoItem::try_from(i).unwrap().try_into_table().unwrap())
                .collect::<Vec<_>>();
            Ok(List::new(tables, token))
        } else {
            Ok(List::new(vec![], token))
        }
    }

    pub async fn query_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        pagination: &ListCursor,
    ) -> Result<List<Table>, TableManagerError> {
        let pk = format!("SHARE#{}#SCHEMA#{}", share_name, schema_name);
        let mut query = self
            .client
            .query()
            .table_name(self.config.table_name.clone())
            .index_name(self.config.index_name.clone())
            .expression_attribute_names("#SK", "SK")
            .expression_attribute_names("#PK", "PK")
            .expression_attribute_values(":sk", AttributeValue::S("TABLE".to_owned()))
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .key_condition_expression("#SK = :sk AND begins_with(#PK, :pk)");

        // Handle pagination requirements and set cursor to correct position in collection
        if let Some(limit) = pagination.max_results() {
            query = query.limit(limit as i32);
        }
        if let Some(cursor) = pagination.try_into().unwrap() {
            query = query.set_exclusive_start_key(Some(cursor.into_start_key()));
        }

        // Fire DynamoDB query
        let query_output = query.send().await.unwrap();

        // Encode cursor position into pagination token
        let token = query_output.last_evaluated_key().and_then(|k| {
            let cursor = DynamoCursor::try_from_last_key(k).unwrap();
            let token = ListCursor::from_cursor(&cursor).unwrap();
            Some(token)
        });

        if let Some(items) = query_output.items() {
            let tables = items
                .iter()
                .map(|i| DynamoItem::try_from(i).unwrap().try_into_table().unwrap())
                .collect::<Vec<_>>();
            Ok(List::new(tables, token))
        } else {
            Ok(List::new(vec![], token))
        }
    }
}

pub struct DynamoConfig {
    table_name: String,
    index_name: String,
}

impl DynamoConfig {
    pub fn new(table_name: impl Into<String>, index_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            index_name: index_name.into(),
        }
    }

    pub fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    pub fn index_name(&self) -> &str {
        self.index_name.as_ref()
    }
}

struct DynamoShareItem {
    inner: Share,
}

impl DynamoShareItem {
    fn into_inner(self) -> Share {
        self.inner
    }
}

impl From<Share> for DynamoShareItem {
    fn from(value: Share) -> Self {
        Self { inner: value }
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for DynamoShareItem {
    type Error = DynamoError;

    fn try_from(value: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let sk = value
            .get("SK")
            .ok_or(DynamoError::InvalidShareItem)?
            .as_s()
            .map_err(|_| DynamoError::InvalidShareItem)?;
        let pk_parts = value
            .get("PK")
            .ok_or(DynamoError::InvalidShareItem)?
            .as_s()
            .map_err(|_| DynamoError::InvalidShareItem)?
            .split("#")
            .collect::<Vec<_>>();

        if pk_parts.len() != 6 {
            return Err(DynamoError::InvalidShareItem);
        }

        match sk.as_str() {
            "SHARE" => {
                let share_name = pk_parts[1].to_owned();
                let share_id = value.get("share_id").and_then(|v| v.as_s().ok().cloned());
                Ok(DynamoShareItem {
                    inner: Share::new(share_name, share_id),
                })
            }
            _ => Err(DynamoError::InvalidShareItem),
        }
    }
}

struct DynamoSchemaItem {
    inner: Schema,
}

struct DynamoTableItem {
    inner: Table,
}

#[derive(Debug)]
enum DynamoItem {
    Share(Share),
    Schema(Schema),
    Table(Table),
}

impl DynamoItem {
    fn try_into_share(self) -> Result<Share, Self> {
        if let Self::Share(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    fn try_into_schema(self) -> Result<Schema, Self> {
        if let Self::Schema(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    fn try_into_table(self) -> Result<Table, Self> {
        if let Self::Table(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for DynamoItem {
    type Error = TableManagerError;

    fn try_from(value: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let sk = value
            .get("SK")
            .ok_or(Self::Error::Other)?
            .as_s()
            .map_err(|_| Self::Error::Other)?;
        let pk = value
            .get("PK")
            .ok_or(Self::Error::Other)?
            .as_s()
            .map_err(|_| Self::Error::Other)?
            .split("#")
            .collect::<Vec<_>>();

        assert_eq!(pk.len(), 6);

        match sk.as_str() {
            "SHARE" => {
                let share_name = pk[1].to_owned();
                let share_id = value.get("share_id").and_then(|v| v.as_s().ok().cloned());

                let share = Share::new(share_name, share_id);
                Ok(DynamoItem::Share(share))
            }
            "SCHEMA" => {
                let share_name = pk[1].to_owned();
                let schema_name = pk[3].to_owned();

                let share_id = value.get("share_id").and_then(|v| v.as_s().ok().cloned());
                let share = Share::new(share_name, share_id);
                let schema = Schema::new(share, schema_name);
                Ok(DynamoItem::Schema(schema))
            }
            "TABLE" => {
                // required properties
                let share_name = pk[1].to_owned();
                let schema_name = pk[3].to_owned();
                let table_name = pk[5].to_owned();
                let storage_path = value
                    .get("storage_path")
                    .ok_or(Self::Error::Other)?
                    .as_s()
                    .map_err(|_| Self::Error::Other)?;

                // optional properties
                let share_id = value.get("share_id").and_then(|v| v.as_s().ok().cloned());
                let table_id = value.get("table_id").and_then(|v| v.as_s().ok().cloned());

                let share = Share::new(share_name, share_id);
                let schema = Schema::new(share.clone(), schema_name);
                let table = Table::new(schema, table_name, storage_path.clone(), table_id, None);
                Ok(DynamoItem::Table(table))
            }
            _ => Err(Self::Error::Other),
        }
    }
}

#[derive(Debug)]
pub enum DynamoError {
    ListCursorNotFound,
    InvalidListCursor,
    InvalidDynamoCursor,
    ShareNotFound { share: String },
    SchemaNotFound { schema: String },
    TableNotFound { table: String },
    InvalidShareItem,
    InvalidSchemaItem,
    InvalidTableItem,
    ServiceError { reason: String },
    Other,
}

#[derive(Serialize, Deserialize)]
struct DynamoCursor {
    pk: String,
    sk: String,
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

#[async_trait]
impl TableManager for DynamoTableManager {
    async fn list_shares(&self, pagination: &ListCursor) -> Result<List<Share>, TableManagerError> {
        self.query_shares(pagination).await
    }

    async fn get_share(&self, share_name: &str) -> Result<Share, TableManagerError> {
        self.get_share(share_name).await
    }

    async fn list_schemas(
        &self,
        share_name: &str,
        pagination: &ListCursor,
    ) -> Result<List<Schema>, TableManagerError> {
        self.query_schemas(share_name, pagination).await
    }

    async fn list_tables_in_share(
        &self,
        share_name: &str,
        pagination: &ListCursor,
    ) -> Result<List<Table>, TableManagerError> {
        self.query_tables_in_share(share_name, pagination).await
    }

    async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        pagination: &ListCursor,
    ) -> Result<List<Table>, TableManagerError> {
        self.query_tables_in_schema(share_name, schema_name, pagination)
            .await
    }

    async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, TableManagerError> {
        self.get_table(share_name, schema_name, table_name).await
    }
}
