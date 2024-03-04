use delta_sharing_server::{auth::ClientId, catalog::postgres::PostgresCatalog};
use sqlx::PgPool;
use testcontainers::{clients::Cli, Container, Image};
use testcontainers_modules::postgres::Postgres;

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
