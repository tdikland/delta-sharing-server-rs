use delta_sharing_server::{
    auth::ClientId,
    catalog::{postgres::PostgresCatalog, Catalog, Pagination, SchemaInfo, ShareInfo, TableInfo},
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

    let catalog = PostgresCatalog::from_pool(pool);
    catalog
}

async fn seed_catalog(catalog: &PostgresCatalog) {
    let anon_client = ClientId::anonymous();
    let known_client = ClientId::known("client1");

    let anon_client_model = catalog.insert_client(anon_client.clone()).await.unwrap();
    let known_client_model = catalog.insert_client(known_client.clone()).await.unwrap();

    // Insert shares
    let share1 = catalog
        .insert_share(ShareInfo::new("share1".to_owned(), None))
        .await
        .unwrap();
    let share2 = catalog
        .insert_share(ShareInfo::new("share2".to_owned(), None))
        .await
        .unwrap();
    let share3 = catalog
        .insert_share(ShareInfo::new("share3".to_owned(), None))
        .await
        .unwrap();

    // Mark the first two shares as publicly shared
    catalog
        .grant_access_to_share(&anon_client_model.id, &share1.id)
        .await
        .unwrap();
    catalog
        .grant_access_to_share(&anon_client_model.id, &share2.id)
        .await
        .unwrap();

    // Mark the last share as privately shared to client1
    catalog
        .grant_access_to_share(&known_client_model.id, &share3.id)
        .await
        .unwrap();

    // Insert schemas
    let schema1 = catalog
        .insert_schema(
            &share1.id,
            SchemaInfo::new("schema1".to_owned(), "share1".to_owned()),
        )
        .await
        .unwrap();
    let schema2 = catalog
        .insert_schema(
            &share1.id,
            SchemaInfo::new("schema2".to_owned(), "share1".to_owned()),
        )
        .await
        .unwrap();

    catalog
        .grant_access_to_schema(&anon_client_model.id, &schema1.id)
        .await
        .unwrap();
    catalog
        .grant_access_to_schema(&anon_client_model.id, &schema2.id)
        .await
        .unwrap();

    // Insert tables
    let table1 = catalog
        .insert_table(
            &schema1.id,
            TableInfo::new(
                "table1".to_owned(),
                "schema1".to_owned(),
                "share1".to_owned(),
                "s3://bucket1/prefix1/".to_owned(),
            ),
        )
        .await
        .unwrap();
    let table2 = catalog
        .insert_table(
            &schema1.id,
            TableInfo::new(
                "table2".to_owned(),
                "schema1".to_owned(),
                "share1".to_owned(),
                "s3://bucket1/prefix2/".to_owned(),
            ),
        )
        .await
        .unwrap();
    let table3 = catalog
        .insert_table(
            &schema2.id,
            TableInfo::new(
                "table1".to_owned(),
                "schema2".to_owned(),
                "share1".to_owned(),
                "s3://bucket2/prefix1/".to_owned(),
            ),
        )
        .await
        .unwrap();

    // Grant access to tables
    catalog
        .grant_access_to_table(&anon_client_model.id, &table1.id)
        .await
        .unwrap();
    catalog
        .grant_access_to_table(&anon_client_model.id, &table2.id)
        .await
        .unwrap();
    catalog
        .grant_access_to_table(&anon_client_model.id, &table3.id)
        .await
        .unwrap();
}
