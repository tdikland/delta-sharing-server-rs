use delta_sharing_server::{
    auth::ClientId,
    catalog::{postgres::PostgresCatalog, Catalog, Pagination},
};
use sqlx::PgPool;
use testcontainers::{clients::Cli, Container, Image};
use testcontainers_modules::postgres::Postgres;

#[tokio::test]
async fn list_shares() {
    let docker = Cli::default();
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;
    seed_catalog(&catalog).await;

    // List public shares
    let anonymous_client = ClientId::anonymous();
    let anon_shares = catalog
        .list_shares(&anonymous_client, &Pagination::default())
        .await
        .unwrap();

    assert_eq!(anon_shares.len(), 2);
    assert!(anon_shares.items().iter().any(|s| s.name() == "share1"));
    assert!(anon_shares.items().iter().any(|s| s.name() == "share2"));
    assert_eq!(anon_shares.next_page_token(), None);

    // List private shares of known client
    let existing_client = ClientId::known("client1");
    let known_shares = catalog
        .list_shares(&existing_client, &Pagination::default())
        .await
        .unwrap();

    assert_eq!(known_shares.len(), 1);
    assert!(known_shares.items().iter().any(|s| s.name() == "share3"));
    assert_eq!(known_shares.next_page_token(), None);

    // List private shares of unknown client yuields no results
    let unknown_client = ClientId::known("client2");
    let unknown_shares = catalog
        .list_shares(&unknown_client, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(unknown_shares.len(), 0);
    assert_eq!(unknown_shares.next_page_token(), None);
}

#[tokio::test]
async fn list_shares_pagination() {
    let docker = Cli::default();
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;
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
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;
    seed_catalog(&catalog).await;
    let client = ClientId::anonymous();

    let schemas = catalog
        .list_schemas(&client, "share1", &Pagination::default())
        .await
        .unwrap();
    assert_eq!(schemas.len(), 2);
    assert!(schemas
        .items()
        .iter()
        .any(|s| s.name() == "schema1" && s.share_name() == "share1"));
    assert!(schemas
        .items()
        .iter()
        .any(|s| s.name() == "schema2" && s.share_name() == "share1"));
    assert_eq!(schemas.next_page_token(), None);
}

#[tokio::test]
async fn list_schemas_pagination() {
    let docker = Cli::default();
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;
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
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;
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
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;
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
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;
    seed_catalog(&catalog).await;
    let client = ClientId::anonymous();

    let tables = catalog
        .list_tables_in_schema(&client, "share1", "schema1", &Pagination::default())
        .await
        .unwrap();
    assert_eq!(tables.items().len(), 2);
    assert_eq!(tables.next_page_token(), None);
}

#[tokio::test]
async fn list_tables_in_schema_pagination() {
    let docker = Cli::default();
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;
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
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;
    seed_catalog(&catalog).await;
    let client = ClientId::anonymous();

    let share = catalog.get_share(&client, "share1").await.unwrap();
    assert_eq!(share.name(), "share1");

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
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;
    seed_catalog(&catalog).await;
    let client = ClientId::anonymous();

    let table = catalog
        .get_table(&client, "share1", "schema1", "table1")
        .await
        .unwrap();
    assert_eq!(table.share_name(), "share1");
    assert_eq!(table.schema_name(), "schema1");
    assert_eq!(table.name(), "table1");
    assert_eq!(table.storage_path(), "s3://bucket1/prefix1/");

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
async fn client_lifecycle() {
    let docker = Cli::default();
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;

    // Insert a client named foo
    let client_id = ClientId::known("foo");
    let client = catalog.insert_client(&client_id).await.unwrap();
    assert_eq!(client.name, "foo");

    // Select the client by name
    let selected_client = catalog.select_client_by_name(&client_id).await.unwrap();
    assert!(selected_client.is_some());
    assert_eq!(selected_client.unwrap().name, "foo");

    // TODO: fail to create the same client twice!

    // Delete client by id
    let result = catalog.delete_client(&client.id).await;
    assert!(result.is_ok());

    // Select the client by name
    let selected_client = catalog.select_client_by_name(&client_id).await.unwrap();
    assert!(selected_client.is_none());
}

#[tokio::test]
async fn share_lifecycle() {
    let docker = Cli::default();
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;

    // Insert a client named foo
    let client_id = ClientId::known("foo");
    let client = catalog.insert_client(&client_id).await.unwrap();
    assert_eq!(client.name, "foo");

    // Insert a share named bar
    let share = catalog.insert_share("bar").await.unwrap();
    assert_eq!(share.name, "bar");

    // Select the share by name (no access granted yet)
    let selected_share = catalog
        .select_share_by_name(&client_id, "bar")
        .await
        .unwrap();
    assert!(selected_share.is_none());

    // Grant access to the share
    catalog
        .grant_access_to_share(&client.id, &share.id)
        .await
        .unwrap();

    // Select the share by name
    let selected_share = catalog
        .select_share_by_name(&client_id, "bar")
        .await
        .unwrap();
    assert!(selected_share.is_some());
    assert_eq!(selected_share.unwrap().name, "bar");

    // Revoke access to the share
    catalog
        .revoke_access_to_share(&client.id, &share.id)
        .await
        .unwrap();

    // Select the share by name
    let selected_share = catalog
        .select_share_by_name(&client_id, "bar")
        .await
        .unwrap();
    assert!(selected_share.is_none());

    // Delete share by id
    let result = catalog.delete_share(&share.id).await;
    assert!(result.is_ok());

    // Select the share by name
    let selected_share = catalog
        .select_share_by_name(&client_id, "bar")
        .await
        .unwrap();
    assert!(selected_share.is_none());
}

#[tokio::test]
async fn schema_lifecycle() {
    let docker = Cli::default();
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;

    // Insert a client named foo
    let client_id = ClientId::known("foo");
    let client = catalog.insert_client(&client_id).await.unwrap();
    assert_eq!(client.name, "foo");

    // Insert a share named bar and grant access to the client
    let share = catalog.insert_share("bar").await.unwrap();
    catalog
        .grant_access_to_share(&client.id, &share.id)
        .await
        .unwrap();
    assert_eq!(share.name, "bar");

    // Insert a schema named baz
    let schema = catalog.insert_schema(&share.id, "baz").await.unwrap();
    assert_eq!(schema.name, "baz");

    // Select the schema by name
    let selected_schema = catalog
        .select_schema_by_name(&client_id, &share.name, "baz")
        .await
        .unwrap();
    assert!(selected_schema.is_none());

    // Grant access to the schema
    catalog
        .grant_access_to_schema(&client.id, &schema.id)
        .await
        .unwrap();

    // Select the schema by name
    let selected_schema = catalog
        .select_schema_by_name(&client_id, &share.name, "baz")
        .await
        .unwrap();
    assert!(selected_schema.is_some());
    assert_eq!(selected_schema.unwrap().name, "baz");

    // Revoke access to the schema
    catalog
        .revoke_access_to_schema(&client.id, &schema.id)
        .await
        .unwrap();

    // Select the schema by name
    let selected_schema = catalog
        .select_schema_by_name(&client_id, &share.name, "baz")
        .await
        .unwrap();
    assert!(selected_schema.is_none());

    // Delete schema by id
    let result = catalog.delete_schema(&schema.id).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn table_lifecycle() {
    let docker = Cli::default();
    let postgres = Postgres::default();
    let container = docker.run(postgres);

    let pool = init_connection(&container).await;
    let catalog = init_catalog(pool).await;

    // Insert a client named foo
    let client_id = ClientId::known("foo");
    let client = catalog.insert_client(&client_id).await.unwrap();
    assert_eq!(client.name, "foo");

    // Insert a share named bar and grant access to the client
    let share = catalog.insert_share("bar").await.unwrap();
    catalog
        .grant_access_to_share(&client.id, &share.id)
        .await
        .unwrap();
    assert_eq!(share.name, "bar");

    // Insert a schema named baz and grant access to the client
    let schema = catalog.insert_schema(&share.id, "baz").await.unwrap();
    catalog
        .grant_access_to_schema(&client.id, &schema.id)
        .await
        .unwrap();
    assert_eq!(schema.name, "baz");

    // Insert a table named qux
    let table = catalog
        .insert_table(&schema.id, "qux", "s3://bucket/prefix")
        .await
        .unwrap();
    assert_eq!(table.name, "qux");

    // Select the table by name
    let selected_table = catalog
        .select_table_by_name(&client_id, &share.name, &schema.name, "qux")
        .await
        .unwrap();
    assert!(selected_table.is_none());

    // Grant access to the table
    catalog
        .grant_access_to_table(&client.id, &table.id)
        .await
        .unwrap();

    // Select the table by name
    let selected_table = catalog
        .select_table_by_name(&client_id, &share.name, &schema.name, "qux")
        .await
        .unwrap();
    assert!(selected_table.is_some());
    assert_eq!(selected_table.unwrap().name, "qux");

    // Revoke access to the table
    catalog
        .revoke_access_to_table(&client.id, &table.id)
        .await
        .unwrap();

    // Select the table by name
    let selected_table = catalog
        .select_table_by_name(&client_id, &share.name, &schema.name, "qux")
        .await
        .unwrap();
    assert!(selected_table.is_none());

    // Delete table by id
    let result = catalog.delete_table(&table.id).await;
    assert!(result.is_ok());
}

async fn init_connection<I: Image>(container: &Container<'_, I>) -> PgPool {
    let url = format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        container.get_host_port_ipv4(5432)
    );
    PgPool::connect(&url).await.unwrap()
}

async fn init_catalog(pool: PgPool) -> PostgresCatalog {
    sqlx::migrate!("tests/migrations/postgres")
        .run(&pool)
        .await
        .unwrap();

    PostgresCatalog::from_pool(pool)
}

async fn seed_catalog(catalog: &PostgresCatalog) {
    let anon_id = ClientId::anonymous();
    let known_id = ClientId::known("client1");

    let anon_client = catalog.insert_client(&anon_id).await.unwrap();
    let known_client = catalog.insert_client(&known_id).await.unwrap();

    // Insert shares
    let share1 = catalog.insert_share("share1").await.unwrap();
    let share2 = catalog.insert_share("share2").await.unwrap();
    let share3 = catalog.insert_share("share3").await.unwrap();

    // Mark the first two shares as publicly shared
    catalog
        .grant_access_to_share(&anon_client.id, &share1.id)
        .await
        .unwrap();
    catalog
        .grant_access_to_share(&anon_client.id, &share2.id)
        .await
        .unwrap();

    // Mark the last share as privately shared to client1
    catalog
        .grant_access_to_share(&known_client.id, &share3.id)
        .await
        .unwrap();

    // Insert schemas
    let schema1 = catalog.insert_schema(&share1.id, "schema1").await.unwrap();
    let schema2 = catalog.insert_schema(&share1.id, "schema2").await.unwrap();

    catalog
        .grant_access_to_schema(&anon_client.id, &schema1.id)
        .await
        .unwrap();
    catalog
        .grant_access_to_schema(&anon_client.id, &schema2.id)
        .await
        .unwrap();

    // Insert tables
    let table1 = catalog
        .insert_table(&schema1.id, "table1", "s3://bucket1/prefix1/")
        .await
        .unwrap();
    let table2 = catalog
        .insert_table(&schema1.id, "table2", "s3://bucket1/prefix2/")
        .await
        .unwrap();
    let table3 = catalog
        .insert_table(&schema2.id, "table3", "s3://bucket2/prefix1/")
        .await
        .unwrap();

    // Grant access to tables
    catalog
        .grant_access_to_table(&anon_client.id, &table1.id)
        .await
        .unwrap();
    catalog
        .grant_access_to_table(&anon_client.id, &table2.id)
        .await
        .unwrap();
    catalog
        .grant_access_to_table(&anon_client.id, &table3.id)
        .await
        .unwrap();
}
