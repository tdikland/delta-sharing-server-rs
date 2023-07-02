//! ShareReader implementation leveraging Postgres as backing store.

use async_trait::async_trait;
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    PgPool, Row,
};
use uuid::Uuid;

use crate::protocol::securable::{Schema, SchemaBuilder, Share, ShareBuilder, Table, TableBuilder};

use super::{List, ListCursor, ShareIoError, ShareReader};

/// ShareReader implementation leveraging Postgres as backing store.
#[derive(Debug)]
pub struct PostgresShareReader {
    pool: PgPool,
}

impl PostgresShareReader {
    /// Create a new PostgresShareReader.
    pub async fn new(connection_url: &str) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(500)
            .connect(connection_url)
            .await
            .expect("Failed to connect to Postgres");

        Self { pool }
    }

    /// Create a new PostgresShareReader from an existing PgPool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Return a reference to the underlying PgPool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Insert a new share into the database.
    pub async fn insert_share(&self, share_name: &str) -> Result<Share, sqlx::Error> {
        let share_id = Uuid::new_v4();
        sqlx::query("INSERT INTO share (id, name) VALUES ($1, $2);")
            .bind(share_id)
            .bind(share_name)
            .execute(&self.pool)
            .await?;

        let share = ShareBuilder::new(share_name)
            .id(share_id.to_string())
            .build();
        Ok(share)
    }

    async fn select_share_by_name(&self, share_name: &str) -> Result<Option<Share>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 
                id::text AS share_id,
                name AS share_name
            FROM share
            WHERE name = $1;
            "#,
        )
        .bind(share_name)
        .fetch_optional(&self.pool)
        .await?
        .map(TryFrom::try_from)
        .transpose()
    }

    async fn select_shares(&self, cursor: &PostgresCursor) -> Result<Vec<Share>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 
                id::text AS share_id,
                name AS share_name
            FROM share
            WHERE id > $1
            ORDER BY id
            LIMIT $2;
            "#,
        )
        .bind(cursor.last_seen_id())
        .bind(cursor.limit())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryFrom::try_from)
        .collect()
    }

    /// Delete a share from the database.
    pub async fn delete_share_by_name(&self, share_name: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM share WHERE name = $1;")
            .bind(share_name)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Delete all shares from the database.
    pub async fn delete_shares(&self) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM share;")
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Insert a new schema into the database.
    pub async fn insert_schema(
        &self,
        share: &Share,
        schema_name: &str,
    ) -> Result<Schema, sqlx::Error> {
        let schema_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO schema (id, name, share_id) 
            VALUES ($1, $2, $3);
            "#,
        )
        .bind(schema_id)
        .bind(schema_name)
        .bind(Uuid::parse_str(share.id().unwrap()).unwrap())
        .execute(&self.pool)
        .await?;

        let schema = SchemaBuilder::new(share.clone(), schema_name)
            .id(schema_id.to_string())
            .build();

        Ok(schema)
    }

    async fn select_schema_by_name(
        &self,
        share_name: &str,
        schema_name: &str,
    ) -> Result<Option<Schema>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 
                share.id::text AS share_id,
                share.name AS share_name,
                "schema".id::text AS schema_id,
                "schema".name AS schema_name
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            WHERE share.name = $1 AND "schema".name = $2;
            "#,
        )
        .bind(share_name)
        .bind(schema_name)
        .fetch_optional(&self.pool)
        .await?
        .map(TryFrom::try_from)
        .transpose()
    }

    async fn select_schemas_by_share_name(
        &self,
        share_name: &str,
        cursor: &PostgresCursor,
    ) -> Result<Vec<Schema>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 
                share.id::text AS share_id,
                share.name AS share_name,
                "schema".id::text AS schema_id,
                "schema".name AS schema_name
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            WHERE share.name = $1 AND "schema".id > $2
            ORDER BY "schema".id
            LIMIT $3;
            "#,
        )
        .bind(share_name)
        .bind(cursor.last_seen_id())
        .bind(cursor.limit())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryFrom::try_from)
        .collect()
    }

    /// Delete a schema from the database.
    pub async fn delete_schema_by_name(&self, schema_name: &str) -> Result<(), sqlx::Error> {
        sqlx::query(r#"DELETE FROM "schema" WHERE name = $1;"#)
            .bind(schema_name)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Delete all schemas from the database.
    pub async fn delete_schemas(&self) -> Result<(), sqlx::Error> {
        sqlx::query(r#"DELETE FROM "schema";"#)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Insert a new table into the database.
    pub async fn insert_table(
        &self,
        schema: &Schema,
        table_name: &str,
        storage_path: &str,
        storage_format: Option<&String>,
    ) -> Result<Table, sqlx::Error> {
        let uuid = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO "table" (id, name, schema_id, storage_path, storage_format) 
            VALUES ($1, $2, $3, $4, $5);
            "#,
        )
        .bind(uuid)
        .bind(table_name)
        .bind(Uuid::parse_str(schema.id().unwrap()).unwrap())
        .bind(storage_path)
        .bind(storage_format)
        .execute(&self.pool)
        .await?;

        let table = TableBuilder::new(schema.clone(), table_name, storage_path)
            .id(uuid.to_string())
            .set_format(storage_format.cloned())
            .build();

        Ok(table)
    }

    async fn select_tables_by_share(
        &self,
        share_name: &str,
        cursor: &PostgresCursor,
    ) -> Result<Vec<Table>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT
                share.id::text AS share_id,
                share.name AS share_name,
                "schema".id::text AS schema_id,
                "schema".name AS schema_name,
                "table".id::text AS table_id,
                "table".name AS table_name,
                "table".storage_path AS storage_path,
                "table".storage_format AS storage_format
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            LEFT JOIN "table" ON "table".schema_id = "schema".id
            WHERE share.name = $1 AND "table".id > $2
            ORDER BY "table".id
            LIMIT $3;
            "#,
        )
        .bind(share_name)
        .bind(cursor.last_seen_id())
        .bind(cursor.limit())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryFrom::try_from)
        .collect()
    }

    async fn select_tables_by_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        cursor: &PostgresCursor,
    ) -> Result<Vec<Table>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT
                share.id::text AS share_id,
                share.name AS share_name,
                "schema".id::text AS schema_id,
                "schema".name AS schema_name,
                "table".id::text AS table_id,
                "table".name AS table_name,
                "table".storage_path AS storage_path,
                "table".storage_format AS storage_format
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            LEFT JOIN "table" ON "table".schema_id = "schema".id
            WHERE share.name = $1 AND "schema".name = $2 AND "table".id > $3
            ORDER BY "table".id
            LIMIT $4;
            "#,
        )
        .bind(share_name)
        .bind(schema_name)
        .bind(cursor.last_seen_id())
        .bind(cursor.limit())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryFrom::try_from)
        .collect()
    }

    async fn select_table_by_name(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<Table>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT
                share.id::text AS share_id,
                share.name AS share_name,
                "schema".id::text AS schema_id,
                "schema".name AS schema_name,
                "table".id::text AS table_id,
                "table".name AS table_name,
                "table".storage_path AS storage_path,
                "table".storage_format AS storage_format
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            LEFT JOIN "table" ON "table".schema_id = "schema".id
            WHERE share.name = $1 AND "schema".name = $2 AND "table".name = $3;
            "#,
        )
        .bind(share_name)
        .bind(schema_name)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await?
        .map(TryFrom::try_from)
        .transpose()
    }

    /// Delete a table from the database.
    pub async fn delete_table_by_name(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            DELETE FROM "table"
            WHERE id IN (
                SELECT "table".id
                FROM share
                LEFT JOIN "schema" ON "schema".share_id = share.id
                LEFT JOIN "table" ON "table".schema_id = "schema".id
                WHERE share.name = $1 AND "schema".name = $2 AND "table".name = $3
            );
            "#,
        )
        .bind(share_name)
        .bind(schema_name)
        .bind(table_name)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Delete all tables from the database.
    pub async fn delete_tables(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            DELETE FROM "table";
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

struct PostgresCursor {
    last_seen_id: Option<Uuid>,
    limit: Option<u32>,
}

impl PostgresCursor {
    pub fn new(last_seen_id: Option<Uuid>, limit: Option<u32>) -> Self {
        Self {
            last_seen_id,
            limit,
        }
    }

    pub fn last_seen_id(&self) -> Uuid {
        match self.last_seen_id {
            Some(id) => id,
            None => Uuid::nil(),
        }
    }

    pub fn limit(&self) -> i32 {
        match self.limit {
            Some(lim) => lim as i32,
            None => 100,
        }
    }
}

impl TryFrom<ListCursor> for PostgresCursor {
    type Error = &'static str;
    fn try_from(cursor: ListCursor) -> Result<Self, Self::Error> {
        let last_seen_id = cursor
            .page_token()
            .map(|token| Uuid::parse_str(token).map_err(|_| "invalid page token"))
            .transpose()?;
        let pg_cursor = PostgresCursor::new(last_seen_id, cursor.max_results());
        Ok(pg_cursor)
    }
}

impl TryFrom<PgRow> for Share {
    type Error = sqlx::Error;

    fn try_from(row: PgRow) -> Result<Self, Self::Error> {
        let name: String = row.try_get("share_name")?;
        let id: String = row.try_get("share_id")?;
        Ok(ShareBuilder::new(name).id(id).build())
    }
}

impl TryFrom<PgRow> for Schema {
    type Error = sqlx::Error;

    fn try_from(row: PgRow) -> Result<Self, Self::Error> {
        let share_id: String = row.try_get("share_id")?;
        let share_name: String = row.try_get("share_name")?;
        let schema_id: String = row.try_get("schema_id")?;
        let schema_name: String = row.try_get("schema_name")?;

        let share = ShareBuilder::new(share_name).id(share_id).build();
        let schema = SchemaBuilder::new(share, schema_name).id(schema_id).build();

        Ok(schema)
    }
}

impl TryFrom<PgRow> for Table {
    type Error = sqlx::Error;

    fn try_from(row: PgRow) -> Result<Self, Self::Error> {
        let share_id: String = row.try_get("share_id")?;
        let share_name: String = row.try_get("share_name")?;
        let schema_id: String = row.try_get("schema_id")?;
        let schema_name: String = row.try_get("schema_name")?;
        let table_id: String = row.try_get("table_id")?;
        let table_name: String = row.try_get("table_name")?;
        let storage_path: String = row.try_get("storage_path")?;
        let storage_format: Option<String> = row.try_get("storage_format")?;

        let share = ShareBuilder::new(share_name).id(share_id).build();
        let schema = SchemaBuilder::new(share, schema_name).id(schema_id).build();
        let table = TableBuilder::new(schema, table_name, storage_path)
            .id(table_id)
            .set_format(storage_format)
            .build();

        Ok(table)
    }
}

#[async_trait]
impl ShareReader for PostgresShareReader {
    async fn list_shares(&self, cursor: &ListCursor) -> Result<List<Share>, ShareIoError> {
        let pg_cursor = PostgresCursor::try_from(cursor.clone())
            .map_err(|_| ShareIoError::MalformedContinuationToken)?;
        let shares = self.select_shares(&pg_cursor).await?;

        let next_page_token = if shares.len() == pg_cursor.limit() as usize {
            shares
                .iter()
                .last()
                .and_then(|s| s.id())
                .map(|id| id.to_string())
        } else {
            None
        };

        Ok(List::new(shares, next_page_token))
    }

    async fn get_share(&self, share_name: &str) -> Result<Share, ShareIoError> {
        self.select_share_by_name(share_name)
            .await?
            .ok_or(ShareIoError::ShareNotFound {
                share_name: share_name.to_string(),
            })
    }

    async fn list_schemas(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Schema>, ShareIoError> {
        let pg_cursor = PostgresCursor::try_from(cursor.clone())
            .map_err(|_| ShareIoError::MalformedContinuationToken)?;
        let schemas = self
            .select_schemas_by_share_name(share_name, &pg_cursor)
            .await?;

        let next_page_token = if schemas.len() == pg_cursor.limit() as usize {
            schemas
                .iter()
                .last()
                .and_then(|s| s.id())
                .map(|id| id.to_string())
        } else {
            None
        };

        Ok(List::new(schemas, next_page_token))
    }

    async fn list_tables_in_share(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Table>, ShareIoError> {
        let pg_cursor = PostgresCursor::try_from(cursor.clone())
            .map_err(|_| ShareIoError::MalformedContinuationToken)?;
        let tables = self.select_tables_by_share(share_name, &pg_cursor).await?;

        let next_page_token = if tables.len() == pg_cursor.limit() as usize {
            tables
                .iter()
                .last()
                .and_then(|s| s.id())
                .map(|id| id.to_string())
        } else {
            None
        };

        Ok(List::new(tables, next_page_token))
    }

    async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Table>, ShareIoError> {
        let pg_cursor = PostgresCursor::try_from(cursor.clone())
            .map_err(|_| ShareIoError::MalformedContinuationToken)?;
        let tables = self
            .select_tables_by_schema(share_name, schema_name, &pg_cursor)
            .await?;

        let next_page_token = if tables.len() == pg_cursor.limit() as usize {
            tables
                .iter()
                .last()
                .and_then(|s| s.id())
                .map(|id| id.to_string())
        } else {
            None
        };

        Ok(List::new(tables, next_page_token))
    }

    async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, ShareIoError> {
        match self
            .select_table_by_name(share_name, schema_name, table_name)
            .await
        {
            Ok(Some(table)) => Ok(table),
            Ok(None) => {
                let share = self.select_share_by_name(share_name).await?;
                let schema = self.select_schema_by_name(share_name, schema_name).await?;
                match (share, schema) {
                    (None, _) => Err(ShareIoError::ShareNotFound {
                        share_name: share_name.to_owned(),
                    }),
                    (Some(_), None) => Err(ShareIoError::SchemaNotFound {
                        share_name: share_name.to_owned(),
                        schema_name: schema_name.to_owned(),
                    }),
                    (Some(_), Some(_)) => Err(ShareIoError::TableNotFound {
                        share_name: share_name.to_owned(),
                        schema_name: schema_name.to_owned(),
                        table_name: table_name.to_owned(),
                    }),
                }
            }
            Err(err) => Err(err.into()),
        }
    }
}

// TODO: Sort out Error handling and conversion
impl From<sqlx::Error> for ShareIoError {
    fn from(err: sqlx::Error) -> Self {
        ShareIoError::Other {
            reason: err.to_string(),
        }
    }
}
