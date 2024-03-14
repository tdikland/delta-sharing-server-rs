use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use super::config::DynamoCatalogConfig;
use crate::catalog::{Schema, Share, ShareReaderError, Table};

pub fn build_share_key(
    client_name: &str,
    share_name: &str,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    HashMap::from_iter([
        (
            config.client_id_attr().to_owned(),
            AttributeValue::S(client_name.to_owned()),
        ),
        (
            config.securable_id_attr().to_owned(),
            AttributeValue::S(format!("SHARE#{share_name}")),
        ),
    ])
}

pub fn build_share_item(
    client_name: &str,
    share_name: &str,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    let mut item = build_share_key(client_name, share_name, config);
    item.insert(
        config.share_name_attr().to_owned(),
        AttributeValue::S(share_name.to_owned()),
    );

    item
}

pub fn item_to_share(
    item: &HashMap<String, AttributeValue>,
    config: &DynamoCatalogConfig,
) -> Result<Share, ShareReaderError> {
    Share::builder()
        .set_name(extract_attr_from_item(item, config.share_name_attr()))
        .build()
}

pub fn build_schema_key(
    client_id: &str,
    share_name: &str,
    schema_name: &str,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    HashMap::from_iter([
        (
            config.client_id_attr().to_owned(),
            AttributeValue::S(client_id.to_owned()),
        ),
        (
            config.securable_id_attr().to_owned(),
            AttributeValue::S(format!("SCHEMA#{share_name}.{schema_name}")),
        ),
    ])
}

pub fn build_schema_item(
    client_name: &str,
    share_name: &str,
    schema_name: &str,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    let mut item = build_schema_key(client_name, share_name, schema_name, config);
    item.insert(
        config.share_name_attr().to_owned(),
        AttributeValue::S(share_name.to_owned()),
    );
    item.insert(
        config.schema_name_attr().to_owned(),
        AttributeValue::S(schema_name.to_owned()),
    );

    item
}

pub fn item_to_schema(
    item: &HashMap<String, AttributeValue>,
    config: &DynamoCatalogConfig,
) -> Result<Schema, ShareReaderError> {
    Schema::builder()
        .set_name(extract_attr_from_item(item, config.schema_name_attr()))
        .set_share_name(extract_attr_from_item(item, config.share_name_attr()))
        .build()
}

pub fn build_table_key(
    client_name: &str,
    share_name: &str,
    schema_name: &str,
    table_name: &str,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    HashMap::from_iter([
        (
            config.client_id_attr().to_owned(),
            AttributeValue::S(client_name.to_owned()),
        ),
        (
            config.securable_id_attr().to_owned(),
            AttributeValue::S(format!("TABLE#{share_name}.{schema_name}.{table_name}")),
        ),
    ])
}

pub fn build_table_item(
    client_name: &str,
    share_name: &str,
    schema_name: &str,
    table_name: &str,
    storage_path: &str,
    config: &DynamoCatalogConfig,
) -> HashMap<String, AttributeValue> {
    let mut item = build_table_key(client_name, share_name, schema_name, table_name, config);
    item.insert(
        config.share_name_attr().to_owned(),
        AttributeValue::S(share_name.to_owned()),
    );
    item.insert(
        config.schema_name_attr().to_owned(),
        AttributeValue::S(schema_name.to_owned()),
    );
    item.insert(
        config.table_name_attr().to_owned(),
        AttributeValue::S(table_name.to_owned()),
    );
    item.insert(
        config.storage_path_attr().to_owned(),
        AttributeValue::S(storage_path.to_owned()),
    );

    item
}

pub fn item_to_table(
    item: &HashMap<String, AttributeValue>,
    config: &DynamoCatalogConfig,
) -> Result<Table, ShareReaderError> {
    Table::builder()
        .set_share_name(extract_attr_from_item(item, config.share_name_attr()))
        .set_schema_name(extract_attr_from_item(item, config.schema_name_attr()))
        .set_name(extract_attr_from_item(item, config.table_name_attr()))
        .set_storage_path(extract_attr_from_item(item, config.storage_path_attr()))
        .build()
}

fn extract_attr_from_item(item: &HashMap<String, AttributeValue>, key: &str) -> Option<String> {
    item.get(key).and_then(|v| v.as_s().ok().cloned())
}

#[cfg(test)]
mod test {
    use crate::auth::RecipientId;

    use super::*;

    #[test]
    fn create_share_key() {
        let config = DynamoCatalogConfig::new("test-table");

        let key = build_share_key("client", "foo", &config);

        assert_eq!(key.len(), 2);
        assert_eq!(key.get("PK").unwrap().as_s().unwrap(), "client");
        assert_eq!(key.get("SK").unwrap().as_s().unwrap(), "SHARE#foo")
    }

    #[test]
    fn create_share_item() {
        let config = DynamoCatalogConfig::new("test-table");

        let item = build_share_item("client", "foo", &config);

        assert_eq!(item.len(), 3);
        assert_eq!(item.get("PK").unwrap().as_s().unwrap(), "client");
        assert_eq!(item.get("SK").unwrap().as_s().unwrap(), "SHARE#foo");
        assert_eq!(item.get("share_name").unwrap().as_s().unwrap(), "foo");
    }

    #[test]
    fn create_share_from_item() {
        let config = DynamoCatalogConfig::new("test-table");
        let item = [("PK", "client"), ("SK", "SHARE#foo"), ("share_name", "foo")]
            .into_iter()
            .map(|(attr, val)| (attr.to_owned(), AttributeValue::S(val.to_owned())))
            .collect::<HashMap<_, _>>();

        let share = item_to_share(&item, &config).unwrap();

        assert_eq!(share, Share::new("foo".to_owned(), None));
    }

    #[test]
    fn create_schema_key() {
        let config = DynamoCatalogConfig::new("test-table");
        let client_id = RecipientId::known("client");
        let share_name = "foo";
        let schema_name = "bar";

        let key = build_schema_key(&client_id, share_name, schema_name, &config);
        assert_eq!(key.len(), 2);
        assert_eq!(key.get("PK").unwrap().as_s().unwrap(), "client");
        assert_eq!(key.get("SK").unwrap().as_s().unwrap(), "SCHEMA#foo.bar")
    }

    #[test]
    fn create_schema_item() {
        let config = DynamoCatalogConfig::new("test-table");

        let item = build_schema_item("client", "foo", "bar", &config);
        assert_eq!(item.len(), 4);
        assert_eq!(item.get("PK").unwrap().as_s().unwrap(), "client");
        assert_eq!(item.get("SK").unwrap().as_s().unwrap(), "SCHEMA#foo.bar");
        assert_eq!(item.get("share_name").unwrap().as_s().unwrap(), "foo");
        assert_eq!(item.get("schema_name").unwrap().as_s().unwrap(), "bar");
    }

    #[test]
    fn create_schema_from_item() {
        let config = DynamoCatalogConfig::new("test-table");
        let item = [
            ("PK", "client"),
            ("SK", "SCHEMA#foo.bar"),
            ("share_name", "foo"),
            ("schema_name", "bar"),
        ]
        .into_iter()
        .map(|(attr, val)| (attr.to_owned(), AttributeValue::S(val.to_owned())))
        .collect::<HashMap<_, _>>();

        let schema = item_to_schema(&item, &config).unwrap();
        assert_eq!(schema, Schema::new("bar".to_owned(), "foo".to_owned()));
    }

    #[test]
    fn create_table_key() {
        let config = DynamoCatalogConfig::new("test-table");

        let key = build_table_key("client", "foo", "bar", "baz", &config);
        assert_eq!(key.len(), 2);
        assert_eq!(key.get("PK").unwrap().as_s().unwrap(), "client");
        assert_eq!(key.get("SK").unwrap().as_s().unwrap(), "TABLE#foo.bar.baz")
    }

    #[test]
    fn create_table_item() {
        let config = DynamoCatalogConfig::new("test-table");

        let key = build_table_item("client", "foo", "bar", "baz", "s3://bucket/prefix", &config);

        println!("{key:?}");

        assert_eq!(key.len(), 6);
        assert_eq!(key.get("PK").unwrap().as_s().unwrap(), "client");
        assert_eq!(key.get("SK").unwrap().as_s().unwrap(), "TABLE#foo.bar.baz");
        assert_eq!(key.get("share_name").unwrap().as_s().unwrap(), "foo");
        assert_eq!(key.get("schema_name").unwrap().as_s().unwrap(), "bar");
        assert_eq!(key.get("table_name").unwrap().as_s().unwrap(), "baz");
        assert_eq!(
            key.get("table_storage_location").unwrap().as_s().unwrap(),
            "s3://bucket/prefix"
        );
    }

    #[test]
    fn create_table_from_item() {
        let config = DynamoCatalogConfig::new("test-table");
        let item = [
            ("PK", "client"),
            ("SK", "TABLE#foo.bar.baz"),
            ("share_name", "foo"),
            ("schema_name", "bar"),
            ("table_name", "baz"),
            ("table_storage_location", "s3://bucket/prefix"),
        ]
        .into_iter()
        .map(|(attr, val)| (attr.to_owned(), AttributeValue::S(val.to_owned())))
        .collect::<HashMap<_, _>>();

        let schema = item_to_table(&item, &config).unwrap();
        assert_eq!(
            schema,
            Table::new(
                "baz".to_owned(),
                "bar".to_owned(),
                "foo".to_owned(),
                "s3://bucket/prefix".to_owned()
            )
        );
    }

    #[test]
    fn extract_from_item() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("str".to_owned(), AttributeValue::S("foo".to_owned()));
        item.insert("int".to_owned(), AttributeValue::N("1".to_owned()));

        let existing_string = extract_attr_from_item(&item, "str");
        assert!(existing_string.is_some());
        assert_eq!(existing_string.unwrap(), "foo");

        let non_existing_string = extract_attr_from_item(&item, "non-existing-key");
        assert!(non_existing_string.is_none());

        let non_string_type = extract_attr_from_item(&item, "int");
        assert!(non_string_type.is_none());
    }
}
