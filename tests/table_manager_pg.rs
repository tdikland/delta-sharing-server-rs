use delta_sharing_server_rs::manager::{postgres::PostgresTableManager, ListCursor, TableManager};
use sqlx::PgPool;
use uuid::Uuid;

async fn insert_share(pool: &PgPool, name: &str) {
    let uuid = Uuid::new_v4();
    sqlx::query("INSERT INTO share (id, name) VALUES ($1, $2)")
        .bind(uuid)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
}

async fn delete_shares(pool: &PgPool) {
    sqlx::query("DELETE FROM share")
        .execute(pool)
        .await
        .unwrap();
}

async fn setup_tables(pool: &PgPool) {
    sqlx::migrate!("src/manager/postgres/migrations")
        .run(pool)
        .await
        .unwrap();

    delete_shares(pool).await;
    insert_share(pool, "share_1").await;
    insert_share(pool, "share_2").await;
    insert_share(pool, "share_3").await;
}

#[tokio::test]
async fn list_tables() {
    let table_manager =
        PostgresTableManager::new("postgres://postgres:postgrespw@localhost:32768").await;
    setup_tables(table_manager.pool()).await;
    let shares = table_manager
        .list_shares(&ListCursor::default())
        .await
        .unwrap();
    assert_eq!(shares.len(), 3);
}


