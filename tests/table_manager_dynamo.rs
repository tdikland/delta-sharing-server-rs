use std::time::Duration;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ProvisionedThroughput, ScalarAttributeType, TableStatus,
};
use delta_sharing_server_rs::manager::dynamo::{DynamoConfig, DynamoTableManager};
use delta_sharing_server_rs::protocol::securables::{Schema, Share, Table};

async fn create_manager(create_table_if_not_exists: bool) -> DynamoTableManager {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_dynamodb::Client::new(&config);

    let table_name = "test-table-manager";
    let index_name = "list-index";

    if !create_table_if_not_exists {
        let table_manager_config = DynamoConfig::new(table_name, index_name);
        let table_manager =
            DynamoTableManager::new(client, table_name.to_owned(), index_name.to_owned());
        return table_manager;
    }

    let describe_table = client.describe_table().table_name(table_name).send().await;
    if describe_table.is_ok() {
        if let Some(t) = describe_table.unwrap().table() {
            if t.table_status().unwrap() != &TableStatus::Active {
                panic!("table_not_active")
            }
            let table_manager_config = DynamoConfig::new(table_name, index_name);
            let table_manager =
                DynamoTableManager::new(client, table_name.to_owned(), index_name.to_owned());
            return table_manager;
        }
    }

    client
        .create_table()
        .table_name(table_name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("PK")
                .attribute_type(ScalarAttributeType::S)
                .build(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("SK")
                .attribute_type(ScalarAttributeType::S)
                .build(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("PK")
                .key_type(KeyType::Hash)
                .build(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("SK")
                .key_type(KeyType::Range)
                .build(),
        )
        .billing_mode(BillingMode::Provisioned)
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(5)
                .write_capacity_units(5)
                .build(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name(index_name)
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("PK")
                        .key_type(KeyType::Hash)
                        .build(),
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("SK")
                        .key_type(KeyType::Range)
                        .build(),
                )
                .provisioned_throughput(
                    ProvisionedThroughput::builder()
                        .read_capacity_units(5)
                        .write_capacity_units(5)
                        .build(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let mut table_status = TableStatus::Creating;
    while table_status != TableStatus::Active {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let describe_table = client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .unwrap();
        table_status = describe_table
            .table()
            .unwrap()
            .table_status()
            .unwrap()
            .clone();
    }

    let table_manager_config = DynamoConfig::new(table_name, index_name);
    let table_manager =
        DynamoTableManager::new(client, table_name.to_owned(), index_name.to_owned());
    table_manager
}

fn build_share(share_number: &str) -> Share {
    let name = format!("share_{}", share_number);
    let id = format!("share_id_{}", share_number);
    Share::new(name, Some(id))
}

fn build_schema(share_number: &str, schema_number: &str) -> Schema {
    let share = build_share(share_number);
    let schema_name = format!("schema_{}", schema_number);
    let id = format!("schema_id_{}", schema_number);
    Schema::new(share, schema_name, Some(id))
}

fn build_table(share_number: &str, schema_number: &str, table_number: &str) -> Table {
    let schema = build_schema(share_number, schema_number);
    let table_name = format!("table_{}", table_number);
    let storage_path = format!(
        "s3://bucket/share_{}/schema_{}/table_{}/",
        share_number, schema_number, table_number
    );
    let table_id = format!("table_id_{}", table_number);
    let table_format = "DELTA".to_owned();
    Table::new(
        schema,
        table_name,
        storage_path,
        Some(table_id),
        Some(table_format),
    )
}

#[tokio::test]
async fn put_get_list() {
    let table_manager = create_manager(true).await;

    // shared-securable-structure
    // |- share_1
    // |  |- schema_1
    // |  |  |- table_1
    // |  |  |- table_2
    // |  |  |- table_3
    // |  |  |- table_4
    // |  |- schema_2
    // |  |  |- table_1
    // |  |  |- table_2
    // |- share_2
    // |  |- schema_1
    // |  |  |- table_1
    // |  |  |- table_2
    // |- share_3

    // Add shares to table manager
    let shares = ["1", "2", "3"]
        .into_iter()
        .map(build_share)
        .collect::<Vec<Share>>();
    for share in shares {
        table_manager.put_share(share).await.unwrap();
    }

    // Add schemas to table manager
    let schemas = [("1", "1"), ("1", "2"), ("2", "1")]
        .into_iter()
        .map(|(share, schema)| build_schema(share, schema))
        .collect::<Vec<Schema>>();
    for schema in schemas {
        table_manager.put_schema(schema).await.unwrap();
    }

    // Add tables to table manager
    let tables = [
        ("1", "1", "1"),
        ("1", "1", "2"),
        ("1", "1", "3"),
        ("1", "1", "4"),
        ("1", "2", "1"),
        ("1", "2", "2"),
        ("2", "1", "1"),
        ("2", "1", "2"),
    ]
    .into_iter()
    .map(|(sh, sch, t)| build_table(sh, sch, t))
    .collect::<Vec<Table>>();
    for table in tables {
        table_manager.put_table(table).await.unwrap();
    }
}
