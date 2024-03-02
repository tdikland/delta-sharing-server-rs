use aws_config::{retry::RetryConfig, BehaviorVersion};
use aws_sdk_dynamodb::{
    types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ProvisionedThroughput,
        ScalarAttributeType,
    },
    Client,
};
use delta_sharing_server::{
    auth::ClientId,
    catalog::{
        dynamo::{DynamoCatalog, DynamoCatalogConfig},
        Catalog, Pagination, SchemaInfo, ShareInfo, TableInfo,
    },
};
use testcontainers::{clients::Cli, Container, Image};
use testcontainers_modules::dynamodb_local::DynamoDb;

#[tokio::test]
async fn test_list_shares() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    seed_catalog(&catalog).await;

    // List public shares
    let anonymous_client = ClientId::anonymous();
    let anon_shares = catalog
        .list_shares(&anonymous_client, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(
        anon_shares.items(),
        &[
            ShareInfo::new("share1".to_owned(), None),
            ShareInfo::new("share2".to_owned(), Some("share_id2".to_owned()))
        ]
    );
    assert_eq!(anon_shares.next_page_token(), None);

    // List private shares of known client
    let existing_client = ClientId::known("client1");
    let existing_shares = catalog
        .list_shares(&existing_client, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(
        existing_shares.items(),
        &[
            ShareInfo::new("share3".to_owned(), None),
            ShareInfo::new("share4".to_owned(), Some("share_id4".to_owned())),
            ShareInfo::new("share5".to_owned(), None)
        ]
    );
    assert_eq!(existing_shares.next_page_token(), None);

    // List private shares of unknown client yuields no results
    let non_existing_client = ClientId::known("client2");
    let non_existing_shares = catalog
        .list_shares(&non_existing_client, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(non_existing_shares.len(), 0);
    assert_eq!(non_existing_shares.next_page_token(), None);
}

#[tokio::test]
async fn test_list_shares_pagination() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    seed_catalog(&catalog).await;
    let client = ClientId::anonymous();

    // List first page of public shares
    let shares_page1 = catalog
        .list_shares(&client, &Pagination::new(Some(1), None))
        .await
        .unwrap();
    assert_eq!(shares_page1.len(), 1);
    assert!(shares_page1.next_page_token().is_some());

    // List second page of public shares
    // Even though all available shares are listed, the next page token is
    // still present, because the max number of items per page is reached.
    let shares_page2 = catalog
        .list_shares(
            &client,
            &Pagination::new(
                Some(1),
                shares_page1.next_page_token().map(ToOwned::to_owned),
            ),
        )
        .await
        .unwrap();
    assert_eq!(shares_page2.len(), 1);
    assert!(shares_page2.next_page_token().is_some());

    // List third page of public shares
    // No more shares are available so there are no shares and the next page token is None.
    let shares_page3 = catalog
        .list_shares(
            &client,
            &Pagination::new(
                Some(1),
                shares_page2.next_page_token().map(ToOwned::to_owned),
            ),
        )
        .await
        .unwrap();
    assert_eq!(shares_page3.len(), 0);
    assert_eq!(shares_page3.next_page_token(), None);
}

#[tokio::test]
async fn list_schemas() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    seed_catalog(&catalog).await;

    let client = ClientId::anonymous();

    let schemas = catalog
        .list_schemas(&client, "share1", &Pagination::default())
        .await
        .unwrap();
    assert_eq!(
        schemas.items(),
        &[
            SchemaInfo::new("schema1".to_owned(), "share1".to_owned()),
            SchemaInfo::new("schema2".to_owned(), "share1".to_owned())
        ]
    );
    assert_eq!(schemas.next_page_token(), None);
}

#[tokio::test]
async fn list_schemas_pagination() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    seed_catalog(&catalog).await;
    let client = ClientId::anonymous();

    let schemas_page = catalog
        .list_schemas(&client, "share1", &Pagination::new(Some(1), None))
        .await
        .unwrap();
    assert_eq!(schemas_page.len(), 1);
    assert!(schemas_page.next_page_token().is_some());
}

#[tokio::test]
async fn list_tables_share() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    seed_catalog(&catalog).await;

    let client = ClientId::anonymous();
    let tables = catalog
        .list_tables_in_share(&client, "share1", &Pagination::default())
        .await
        .unwrap();
    assert_eq!(tables.len(), 3);
    assert_eq!(tables.next_page_token(), None);
}

#[tokio::test]
async fn list_tables_share_pagination() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    seed_catalog(&catalog).await;

    let client = ClientId::anonymous();
    let tables_page = catalog
        .list_tables_in_share(&client, "share1", &Pagination::new(Some(1), None))
        .await
        .unwrap();
    assert_eq!(tables_page.len(), 1);
    assert!(tables_page.next_page_token().is_some());
}

#[tokio::test]
async fn list_tables_in_schema() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    seed_catalog(&catalog).await;

    let client = ClientId::anonymous();
    let tables = catalog
        .list_tables_in_schema(&client, "share1", "schema1", &Pagination::default())
        .await
        .unwrap();
    assert_eq!(
        tables.items(),
        &[
            TableInfo::new(
                "table1".to_owned(),
                "schema1".to_owned(),
                "share1".to_owned(),
                "s3://bucket1/path1".to_owned(),
            ),
            TableInfo::new(
                "table2".to_owned(),
                "schema1".to_owned(),
                "share1".to_owned(),
                "s3://bucket1/path1".to_owned(),
            )
        ]
    );
    assert_eq!(tables.next_page_token(), None);
}

#[tokio::test]
async fn list_tables_in_schema_pagination() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    seed_catalog(&catalog).await;

    let client = ClientId::anonymous();
    let tables_page = catalog
        .list_tables_in_schema(
            &client,
            "share1",
            "schema1",
            &Pagination::new(Some(1), None),
        )
        .await
        .unwrap();
    assert_eq!(tables_page.len(), 1);
    assert!(tables_page.next_page_token().is_some());
}

#[tokio::test]
async fn get_share() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    seed_catalog(&catalog).await;

    let client = ClientId::anonymous();
    let share = catalog.get_share(&client, "share1").await.unwrap();
    assert_eq!(share, ShareInfo::new("share1".to_owned(), None));

    let share_not_found_error = catalog
        .get_share(&client, "does-not-exist")
        .await
        .unwrap_err();
    assert_eq!(
        share_not_found_error.to_string(),
        "share `does-not-exist` could not be found"
    );
}

#[tokio::test]
async fn get_table() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    seed_catalog(&catalog).await;

    let client = ClientId::anonymous();
    let table = catalog
        .get_table(&client, "share1", "schema1", "table1")
        .await
        .unwrap();
    assert_eq!(
        table,
        TableInfo::new(
            "table1".to_owned(),
            "schema1".to_owned(),
            "share1".to_owned(),
            "s3://bucket1/path1".to_owned()
        )
    );

    let table_not_found_error = catalog
        .get_table(&client, "share1", "schema1", "does-not-exist")
        .await
        .unwrap_err();
    assert_eq!(
        table_not_found_error.to_string(),
        "table `share1.schema1.does-not-exist` could not be found"
    );
}

#[tokio::test]
async fn put_schema_without_share_error() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;

    // Writing a schema without the parent share should fail
    let res = catalog
        ._put_schema(
            ClientId::anonymous(),
            SchemaInfo::new("schema1".to_owned(), "share1".to_owned()),
        )
        .await;
    assert!(res.is_err());

    // Writing a schema with the parent share should succeed
    catalog
        ._put_share(
            ClientId::anonymous(),
            ShareInfo::new("share1".to_owned(), None),
        )
        .await
        .unwrap();

    let res = catalog
        ._put_schema(
            ClientId::anonymous(),
            SchemaInfo::new("schema1".to_owned(), "share1".to_owned()),
        )
        .await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn put_table_without_schema_error() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;

    // Writing a table without the parent schema should fail
    let res = catalog
        ._put_table(
            ClientId::anonymous(),
            TableInfo::new(
                "table1".to_owned(),
                "schema1".to_owned(),
                "share1".to_owned(),
                "s3://bucket1/path1".to_owned(),
            ),
        )
        .await;
    assert!(res.is_err());

    // Writing a table with the parent schema should succeed
    catalog
        ._put_share(
            ClientId::anonymous(),
            ShareInfo::new("share1".to_owned(), None),
        )
        .await
        .unwrap();
    catalog
        ._put_schema(
            ClientId::anonymous(),
            SchemaInfo::new("schema1".to_owned(), "share1".to_owned()),
        )
        .await
        .unwrap();

    let res = catalog
        ._put_table(
            ClientId::anonymous(),
            TableInfo::new(
                "table1".to_owned(),
                "schema1".to_owned(),
                "share1".to_owned(),
                "s3://bucket1/path1".to_owned(),
            ),
        )
        .await;
    assert!(res.is_ok());
}

#[ignore]
#[tokio::test]
async fn delete_schema_with_tables_error() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    let client_id = ClientId::anonymous();

    // Writing a table with the parent schema should succeed
    catalog
        ._put_share(client_id.clone(), ShareInfo::new("share1".to_owned(), None))
        .await
        .unwrap();
    catalog
        ._put_schema(
            client_id.clone(),
            SchemaInfo::new("schema1".to_owned(), "share1".to_owned()),
        )
        .await
        .unwrap();
    catalog
        ._put_table(
            client_id.clone(),
            TableInfo::new(
                "table1".to_owned(),
                "schema1".to_owned(),
                "share1".to_owned(),
                "s3://bucket1/path1".to_owned(),
            ),
        )
        .await
        .unwrap();

    // Deleting a schema with tables should fail
    let res = catalog
        ._delete_schema(&client_id, "schema1", "share1")
        .await;
    assert!(res.is_err());

    // Deleting a table should allow the schema to be deleted
    catalog
        ._delete_table(&client_id, "table1", "schema1", "share1")
        .await
        .unwrap();

    let res = catalog
        ._delete_schema(&client_id, "schema1", "share1")
        .await;
    assert!(res.is_ok());
}

#[ignore]
#[tokio::test]
async fn delete_share_with_schemas_error() {
    let docker = Cli::default();
    let dynamo = DynamoDb::default();
    let container = docker.run(dynamo);

    let client = init_client(&container).await;
    let catalog = init_catalog(client, "test-table").await;
    let client_id = ClientId::anonymous();

    // Writing a schema with the parent share should succeed
    catalog
        ._put_share(client_id.clone(), ShareInfo::new("share1".to_owned(), None))
        .await
        .unwrap();
    catalog
        ._put_schema(
            client_id.clone(),
            SchemaInfo::new("schema1".to_owned(), "share1".to_owned()),
        )
        .await
        .unwrap();

    // Deleting a share with schemas should fail
    let res = catalog._delete_share(&client_id, "share1").await;
    assert!(res.is_err());

    // Deleting a schema should allow the share to be deleted
    catalog
        ._delete_schema(&client_id, "schema1", "share1")
        .await
        .unwrap();

    let res = catalog._delete_share(&client_id, "share1").await;
    assert!(res.is_ok());
}

async fn init_client<I: Image>(container: &Container<'_, I>) -> Client {
    let endpoint_uri = format!("http://127.0.0.1:{}", container.get_host_port_ipv4(8000));
    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint_uri)
        .test_credentials()
        .retry_config(RetryConfig::standard())
        .load()
        .await;
    Client::new(&shared_config)
}

async fn init_catalog(client: Client, table_name: &str) -> DynamoCatalog {
    let table_name = String::from(table_name);

    // Create DynamoDB table
    let mut success = false;
    let mut retries = 0;
    while !success && retries < 25 {
        success = create_table(&client, &table_name).await;
        retries += 1;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    if retries > 6 {
        panic!("Create table after {} tries", retries);
    }

    let config = DynamoCatalogConfig::new(table_name);
    DynamoCatalog::new(client, config)
}

async fn create_table(client: &Client, table_name: &str) -> bool {
    client
        .create_table()
        .table_name(table_name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("PK")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("SK")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("PK")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("SK")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::Provisioned)
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(5)
                .write_capacity_units(5)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .is_ok()
}

async fn seed_catalog(catalog: &DynamoCatalog) {
    let auth_client = ClientId::known("client1");
    let anon_client = ClientId::anonymous();

    // Create public shares
    catalog
        ._put_share(
            anon_client.clone(),
            ShareInfo::new("share1".to_owned(), None),
        )
        .await
        .unwrap();
    catalog
        ._put_share(
            anon_client.clone(),
            ShareInfo::new("share2".to_owned(), Some("share_id2".to_owned())),
        )
        .await
        .unwrap();

    // Add schemas to public shares
    catalog
        ._put_schema(
            anon_client.clone(),
            SchemaInfo::new("schema1".to_owned(), "share1".to_owned()),
        )
        .await
        .unwrap();
    catalog
        ._put_schema(
            anon_client.clone(),
            SchemaInfo::new("schema2".to_owned(), "share1".to_owned()),
        )
        .await
        .unwrap();

    // Add tables to public schemas
    catalog
        ._put_table(
            anon_client.clone(),
            TableInfo::new(
                "table1".to_owned(),
                "schema1".to_owned(),
                "share1".to_owned(),
                "s3://bucket1/path1".to_owned(),
            ),
        )
        .await
        .unwrap();
    catalog
        ._put_table(
            anon_client.clone(),
            TableInfo::new(
                "table2".to_owned(),
                "schema1".to_owned(),
                "share1".to_owned(),
                "s3://bucket1/path1".to_owned(),
            ),
        )
        .await
        .unwrap();
    catalog
        ._put_table(
            anon_client.clone(),
            TableInfo::new(
                "table1".to_owned(),
                "schema2".to_owned(),
                "share1".to_owned(),
                "s3://bucket1/path1".to_owned(),
            ),
        )
        .await
        .unwrap();

    // Create private shares
    catalog
        ._put_share(
            auth_client.clone(),
            ShareInfo::new("share3".to_owned(), None),
        )
        .await
        .unwrap();
    catalog
        ._put_share(
            auth_client.clone(),
            ShareInfo::new("share4".to_owned(), Some("share_id4".to_owned())),
        )
        .await
        .unwrap();
    catalog
        ._put_share(
            auth_client.clone(),
            ShareInfo::new("share5".to_owned(), None),
        )
        .await
        .unwrap();
}
