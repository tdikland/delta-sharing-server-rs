use async_trait::async_trait;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

use crate::protocol::securables::{Schema, Share, Table};

use super::{List, ListCursor, TableManager, TableManagerError};
pub struct PostgresTableManager {
    pool: PgPool,
}

impl PostgresTableManager {
    pub async fn new(connection_url: &str) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(connection_url)
            .await
            .expect("Failed to connect to Postgres");

        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

// TODO: implement pagination

#[async_trait]
impl TableManager for PostgresTableManager {
    async fn list_shares(&self, _cursor: &ListCursor) -> Result<List<Share>, TableManagerError> {
        let shares: Vec<Share> = sqlx::query_as(
            r#"
            SELECT name, id::text
            FROM share
            ORDER BY name
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(List::new(shares, None))
    }

    async fn get_share(&self, share_name: &str) -> Result<Share, TableManagerError> {
        let share: Option<Share> = sqlx::query_as(
            r#"
            SELECT name, id::text
            FROM share
            WHERE name = $1
            "#,
        )
        .bind(share_name)
        .fetch_optional(&self.pool)
        .await?;

        match share {
            Some(share) => Ok(share),
            None => Err(TableManagerError::ShareNotFound {
                share_name: share_name.to_string(),
            }),
        }
    }

    async fn list_schemas(
        &self,
        share_name: &str,
        _cursor: &ListCursor,
    ) -> Result<List<Schema>, TableManagerError> {
        let schemas: Vec<Schema> = sqlx::query(
            r#"
            SELECT "schema".name
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            WHERE share.name = $1
            ORDER BY name
            "#,
        )
        .bind(share_name)
        .fetch_all(&self.pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let name: String = row.get(0);
                    let share = Share::new("share_1".to_owned(), Some("share_1_id".to_owned()));
                    Schema::new(share, name)
                })
                .collect::<Vec<Schema>>()
        })?;

        Ok(List::new(schemas, None))
    }

    async fn list_tables_in_share(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Table>, TableManagerError> {
        let tables: Vec<Table> = sqlx::query(
            r#"
            SELECT "table".name
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            LEFT JOIN "table" ON "table".schema_id = "schema".id
            WHERE share.name = $1
            ORDER BY name
            "#,
        )
        .bind(share_name)
        .fetch_all(&self.pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let name: String = row.get(0);
                    let share = Share::new("share_1".to_owned(), Some("share_1_id".to_owned()));
                    let schema = Schema::new(share, "schema_1".to_owned());
                    Table::new(schema, name, "s3://foo/bar".to_owned(), None, None)
                })
                .collect::<Vec<Table>>()
        })?;

        Ok(List::new(tables, None))
    }

    async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Table>, TableManagerError> {
        // let tables: Vec<Table> = sqlx::query(
        //     r#"
        //     SELECT "table".name,
        //     FROM share
        //     LEFT JOIN "schema" ON "schema".share_id = share.id
        //     LEFT JOIN "table" ON "table".schema_id = "schema".id
        //     WHERE share.name = $1 AND "schema".name = $2
        //     ORDER BY name
        //     "#,
        // );

        todo!()
    }

    async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, TableManagerError> {
        todo!()
    }
}

impl From<sqlx::Error> for TableManagerError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            _ => TableManagerError::Other {
                reason: err.to_string(),
            },
        }
    }
}
