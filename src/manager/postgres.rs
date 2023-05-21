
pub struct PostgresTableManager {
    pool: PgPool,
}

impl PostgresTableManager {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
