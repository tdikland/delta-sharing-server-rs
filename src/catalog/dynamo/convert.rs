use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::{
    auth::ClientId,
    catalog::{CatalogError, Page, SchemaInfo, ShareInfo, TableInfo},
};

use super::{config::DynamoCatalogConfig, pagination::key_to_token};

pub fn to_share_key(
    client_id: &ClientId,
    share_name: &str,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::with_capacity(2);
    key.insert(
        config.client_id().to_owned(),
        AttributeValue::S(client_id.to_string()),
    );
    key.insert(
        config.securable().to_owned(),
        AttributeValue::S(format!("SHARE#{}", share_name)),
    );

    key
}

pub fn to_share_item(
    client_id: ClientId,
    share: ShareInfo,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::with_capacity(4);
    let key = to_share_key(&client_id, share.name(), config);
    item.extend(key);
    item.insert(
        config.share_name().to_owned(),
        AttributeValue::S(share.name().to_owned()),
    );
    if let Some(share_id) = share.id() {
        item.insert(
            config.share_id().to_owned(),
            AttributeValue::S(share_id.to_owned()),
        );
    }

    item
}

pub fn to_share_info(
    item: &HashMap<String, AttributeValue>,
    config: &DynamoCatalogConfig,
) -> Result<ShareInfo, CatalogError> {
    let share_name = extract_from_item(item, config.share_name())?;
    let share_id = extract_from_item_opt(item, config.share_id());

    Ok(ShareInfo::new(share_name, share_id))
}

pub fn to_share_info_page(
    items: &[HashMap<String, AttributeValue>],
    last_key: Option<&HashMap<String, AttributeValue>>,
    config: &DynamoCatalogConfig,
) -> Result<Page<ShareInfo>, CatalogError> {
    let shares = items
        .iter()
        .map(|item| to_share_info(item, config))
        .collect::<Result<Vec<ShareInfo>, CatalogError>>()?;

    Ok(Page::new(shares, last_key.map(key_to_token)))
}

pub fn to_schema_key(
    client_id: &ClientId,
    share_name: &str,
    schema_name: &str,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::with_capacity(2);
    key.insert(
        config.client_id().to_owned(),
        AttributeValue::S(client_id.to_string()),
    );
    key.insert(
        config.securable().to_owned(),
        AttributeValue::S(format!("SCHEMA#{}.{}", share_name, schema_name)),
    );

    key
}

pub fn to_schema_item(
    client_id: ClientId,
    schema: SchemaInfo,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::with_capacity(4);
    let key = to_schema_key(&client_id, schema.share_name(), schema.name(), config);
    item.extend(key);
    item.insert(
        config.share_name().to_owned(),
        AttributeValue::S(schema.share_name().to_owned()),
    );
    item.insert(
        config.schema_name().to_owned(),
        AttributeValue::S(schema.name().to_owned()),
    );

    item
}

pub fn to_schema_info(
    item: &HashMap<String, AttributeValue>,
    config: &DynamoCatalogConfig,
) -> Result<SchemaInfo, CatalogError> {
    let share_name = extract_from_item(item, config.share_name())?;
    let schema_name = extract_from_item(item, config.schema_name())?;

    Ok(SchemaInfo::new(schema_name, share_name))
}

pub fn to_schema_info_page(
    items: &[HashMap<String, AttributeValue>],
    last_key: Option<&HashMap<String, AttributeValue>>,
    config: &DynamoCatalogConfig,
) -> Result<Page<SchemaInfo>, CatalogError> {
    let schemas = items
        .iter()
        .map(|item| to_schema_info(item, config))
        .collect::<Result<Vec<SchemaInfo>, CatalogError>>()?;

    Ok(Page::new(schemas, last_key.map(key_to_token)))
}

pub fn to_table_key(
    client_id: &ClientId,
    share_name: &str,
    schema_name: &str,
    table_name: &str,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::with_capacity(2);
    key.insert(
        config.client_id().to_owned(),
        AttributeValue::S(client_id.to_string()),
    );
    key.insert(
        config.securable().to_owned(),
        AttributeValue::S(format!(
            "TABLE#{}.{}.{}",
            share_name, schema_name, table_name
        )),
    );

    key
}

pub fn to_table_item(
    client_id: ClientId,
    table: TableInfo,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::with_capacity(4);
    let key = to_table_key(
        &client_id,
        table.share_name(),
        table.schema_name(),
        table.name(),
        config,
    );
    item.extend(key);
    item.insert(
        config.share_name().to_owned(),
        AttributeValue::S(table.share_name().to_owned()),
    );
    item.insert(
        config.schema_name().to_owned(),
        AttributeValue::S(table.schema_name().to_owned()),
    );
    item.insert(
        config.table_name().to_owned(),
        AttributeValue::S(table.name().to_owned()),
    );
    item.insert(
        config.table_storage_location().to_owned(),
        AttributeValue::S(table.storage_path().to_owned()),
    );

    item
}

pub fn to_table_info(
    item: &HashMap<String, AttributeValue>,
    config: &DynamoCatalogConfig,
) -> Result<TableInfo, CatalogError> {
    let share_name = extract_from_item(item, config.share_name())?;
    let schema_name = extract_from_item(item, config.schema_name())?;
    let table_name = extract_from_item(item, config.table_name())?;
    let storage_location = extract_from_item(item, config.table_storage_location())?;
    let _id = extract_from_item_opt(item, config.table_id());
    let _share_id = extract_from_item_opt(item, config.share_id());

    Ok(TableInfo::new(
        table_name,
        schema_name,
        share_name,
        storage_location,
    ))
}

pub fn to_table_info_page(
    items: &[HashMap<String, AttributeValue>],
    last_key: Option<&HashMap<String, AttributeValue>>,
    config: &DynamoCatalogConfig,
) -> Result<Page<TableInfo>, CatalogError> {
    let tables = items
        .iter()
        .map(|item| to_table_info(item, config))
        .collect::<Result<Vec<TableInfo>, CatalogError>>()?;

    Ok(Page::new(tables, last_key.map(key_to_token)))
}

fn extract_from_item(
    item: &HashMap<String, AttributeValue>,
    key: &str,
) -> Result<String, CatalogError> {
    item.get(key)
        .ok_or(CatalogError::internal(format!(
            "attribute `{}` not found in item",
            key
        )))?
        .as_s()
        .map_err(|_| CatalogError::internal(format!("attribute `{}` was not a string", key)))
        .cloned()
}

// TODO: this is also falliable! i.e. wrong underlying datatype
fn extract_from_item_opt(item: &HashMap<String, AttributeValue>, key: &str) -> Option<String> {
    item.get(key).and_then(|v| v.as_s().ok().cloned())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn share_key() {
        let config = DynamoCatalogConfig::new("test-table");
        let client_id = ClientId::known("client");
        let share_name = "foo";

        let key = to_share_key(&client_id, share_name, &config);
        assert_eq!(key.len(), 2);
        assert_eq!(key.get("PK").unwrap().as_s().unwrap(), "client");
        assert_eq!(key.get("SK").unwrap().as_s().unwrap(), "SHARE#foo")
    }

    #[test]
    fn schema_key() {
        let config = DynamoCatalogConfig::new("test-table");
        let client_id = ClientId::known("client");
        let share_name = "foo";
        let schema_name = "bar";

        let key = to_schema_key(&client_id, share_name, schema_name, &config);
        assert_eq!(key.len(), 2);
        assert_eq!(key.get("PK").unwrap().as_s().unwrap(), "client");
        assert_eq!(key.get("SK").unwrap().as_s().unwrap(), "SCHEMA#foo.bar")
    }

    #[test]
    fn share_item() {
        let config = DynamoCatalogConfig::new("test-table");
        let client_id = ClientId::known("client");
        let share = ShareInfo::new("foo".to_owned(), Some("id".to_owned()));

        let item = to_share_item(client_id, share, &config);
        assert_eq!(item.len(), 4);
        assert_eq!(item.get("PK").unwrap().as_s().unwrap(), "client");
        assert_eq!(item.get("SK").unwrap().as_s().unwrap(), "SHARE#foo");
        assert_eq!(item.get("share_name").unwrap().as_s().unwrap(), "foo");
        assert_eq!(item.get("share_id").unwrap().as_s().unwrap(), "id");
    }

    #[test]
    fn schema_item() {
        let config = DynamoCatalogConfig::new("test-table");
        let client_id = ClientId::known("client");
        let schema = SchemaInfo::new("bar".to_owned(), "foo".to_owned());

        let item = to_schema_item(client_id, schema, &config);
        assert_eq!(item.len(), 4);
        assert_eq!(item.get("PK").unwrap().as_s().unwrap(), "client");
        assert_eq!(item.get("SK").unwrap().as_s().unwrap(), "SCHEMA#foo.bar");
        assert_eq!(item.get("share_name").unwrap().as_s().unwrap(), "foo");
        assert_eq!(item.get("schema_name").unwrap().as_s().unwrap(), "bar");
    }
}
