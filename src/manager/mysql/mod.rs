use sqlx::mysql::MySqlPoolOptions;
use sqlx::MySqlPool;

pub struct MySqlTableManager {
    pool: MySqlPool,
}

impl MySqlTableManager {
    pub async fn new(connection_url: &str) -> Self {
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(connection_url)
            .await
            .expect("Failed to connect to Postgres");

        Self { pool }
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
}
