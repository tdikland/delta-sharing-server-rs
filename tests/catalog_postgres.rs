use delta_sharing_server::{
    auth::ClientId,
    catalog::{postgres::PostgresCatalog, Catalog, Pagination, ShareInfo},
};
use sqlx::PgPool;
use testcontainers::{clients::Cli, Container, Image};
use testcontainers_modules::postgres::Postgres;
use tracing_subscriber;

#[tokio::test]
async fn test_list_shares() {
    tracing_subscriber::fmt::init();

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
}
