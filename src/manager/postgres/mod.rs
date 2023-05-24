use async_trait::async_trait;
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    PgPool, Postgres, Row,
};
use uuid::Uuid;

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

    pub async fn insert_share(&self, share_name: &str) -> Result<Share, sqlx::Error> {
        let share_id = Uuid::new_v4();
        sqlx::query("INSERT INTO share (id, name) VALUES ($1, $2)")
            .bind(share_id)
            .bind(share_name)
            .execute(&self.pool)
            .await?;

        Ok(Share::new(
            share_name.to_string(),
            Some(share_id.to_string()),
        ))
    }

    pub async fn select_share_by_name(
        &self,
        share_name: &str,
    ) -> Result<Option<Share>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 
                name AS share_name, 
                id::text AS share_id
            FROM share
            WHERE name = $1
            "#,
        )
        .bind(share_name)
        .fetch_optional(&self.pool)
        .await?
        .map(TryFrom::try_from)
        .transpose()
    }

    pub async fn select_shares(&self, cursor: &PostgresCursor) -> Result<Vec<Share>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 
                name AS share_name, 
                id::text AS share_id
            FROM share
            WHERE id > $1
            ORDER BY id
            LIMIT $2
            "#,
        )
        .bind(cursor.last_seen_id())
        .bind(cursor.limit())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryFrom::try_from)
        .collect::<Result<Vec<Share>, _>>()
    }

    pub async fn insert_schema(
        &self,
        share: &Share,
        schema_name: &str,
    ) -> Result<Schema, sqlx::Error> {
        let schema_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO schema (id, name, share_id) 
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(schema_id)
        .bind(schema_name)
        .bind(share.id())
        .execute(&self.pool)
        .await?;

        Ok(Schema::new(share.clone(), schema_name.to_string()))
    }

    pub async fn select_schemas_by_share_name(
        &self,
        share_name: &str,
        cursor: &PostgresCursor,
    ) -> Result<Vec<Schema>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 
                "schema".name AS schema_name,
                share.name AS share_name,
                share.id::text AS share_id
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            WHERE share.name = $1 AND "schema".id > $2
            ORDER BY "schema".id
            LIMIT $3
            "#,
        )
        .bind(share_name)
        .bind(cursor.last_seen_id())
        .bind(cursor.limit())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryFrom::try_from)
        .collect::<Result<Vec<Schema>, _>>()
    }
}

pub struct PostgresCursor {
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

impl TryFrom<PgRow> for Share {
    type Error = sqlx::Error;

    fn try_from(row: PgRow) -> Result<Self, Self::Error> {
        let name: String = row.try_get("share_name")?;
        let id: String = row.try_get("share_id")?;
        Ok(Share::new(name, Some(id)))
    }
}

impl TryFrom<PgRow> for Schema {
    type Error = sqlx::Error;

    fn try_from(row: PgRow) -> Result<Self, Self::Error> {
        let share_id: String = row.try_get("share_id")?;
        let share_name: String = row.try_get("share_name")?;
        let schema_name: String = row.try_get("schema_name")?;
        let share = Share::new(share_name, Some(share_id));
        Ok(Schema::new(share, schema_name))
    }
}

impl TryFrom<PgRow> for Table {
    type Error = sqlx::Error;

    fn try_from(row: PgRow) -> Result<Self, Self::Error> {
        let share_id: String = row.try_get("share_id")?;
        let share_name: String = row.try_get("share_name")?;
        let schema_name: String = row.try_get("schema_name")?;
        let table_name: String = row.try_get("table_name")?;
        let table_id: String = row.try_get("table_id")?;
        let storage_path: String = row.try_get("storage_path")?;
        let storage_format: Option<String> = row.try_get("storage_format")?;

        let share = Share::new(share_name, Some(share_id));
        let schema = Schema::new(share, schema_name);
        Ok(Table::new(
            schema,
            table_name,
            storage_path,
            Some(table_id),
            storage_format,
        ))
    }
}

#[async_trait]
impl TableManager for PostgresTableManager {
    async fn list_shares(&self, _cursor: &ListCursor) -> Result<List<Share>, TableManagerError> {
        let shares: Vec<Share> = sqlx::query(
            r#"
            SELECT name, id::text
            FROM share
            ORDER BY name
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let name: String = row.get(0);
                    let id: String = row.get(1);
                    Share::new(name, Some(id))
                })
                .collect::<Vec<Share>>()
        })?;

        Ok(List::new(shares, None))
    }

    async fn get_share(&self, share_name: &str) -> Result<Share, TableManagerError> {
        let share: Option<Share> = sqlx::query(
            r#"
            SELECT name, id::text
            FROM share
            WHERE name = $1
            "#,
        )
        .bind(share_name)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| {
            let name: String = row.get(0);
            let id: String = row.get(1);
            Share::new(name, Some(id))
        });

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
            SELECT 
                "schema".name AS schema_name,
                share.name AS share_name,
                share.id::text AS share_id
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            WHERE share.name = $1
            ORDER BY schema_name
            "#,
        )
        .bind(share_name)
        .fetch_all(&self.pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let share_id = row.get("share_id");
                    let share_name = row.get("share_name");
                    let schema_name = row.get("schema_name");
                    let share = Share::new(share_name, Some(share_id));
                    Schema::new(share, schema_name)
                })
                .collect()
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
            SELECT
                share.name AS share_name,
                share.id::text AS share_id,
                "schema".name AS schema_name,
                "table".name AS table_name,
                "table".id::text AS table_id,
                "table".storage_path AS storage_path,
                "table".storage_format AS storage_format
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            LEFT JOIN "table" ON "table".schema_id = "schema".id
            WHERE share.name = $1
            ORDER BY table_id
            "#,
        )
        .bind(share_name)
        .fetch_all(&self.pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let share_name = row.get("share_name");
                    let share_id = row.get("share_id");
                    let schema_name = row.get("schema_name");
                    let table_name = row.get("table_name");
                    let table_id = row.get("table_id");
                    let storage_path = row.get("storage_path");
                    let storage_format = row.get("storage_format");

                    let share = Share::new(share_name, Some(share_id));
                    let schema = Schema::new(share, schema_name);
                    Table::new(
                        schema,
                        table_name,
                        storage_path,
                        Some(table_id),
                        storage_format,
                    )
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
        let tables: Vec<Table> = sqlx::query(
            r#"
            SELECT
                share.name AS share_name,
                share.id::text AS share_id,
                "schema".name AS schema_name,
                "table".name AS table_name,
                "table".id::text AS table_id,
                "table".storage_path AS storage_path,
                "table".storage_format AS storage_format
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            LEFT JOIN "table" ON "table".schema_id = "schema".id
            WHERE share.name = $1
            AND "schema".name = $2
            ORDER BY table_id
            "#,
        )
        .bind(share_name)
        .bind(schema_name)
        .fetch_all(&self.pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let share_name = row.get("share_name");
                    let share_id = row.get("share_id");
                    let schema_name = row.get("schema_name");
                    let table_name = row.get("table_name");
                    let table_id = row.get("table_id");
                    let storage_path = row.get("storage_path");
                    let storage_format = row.get("storage_format");

                    let share = Share::new(share_name, Some(share_id));
                    let schema = Schema::new(share, schema_name);
                    Table::new(
                        schema,
                        table_name,
                        storage_path,
                        Some(table_id),
                        storage_format,
                    )
                })
                .collect::<Vec<Table>>()
        })?;

        Ok(List::new(tables, None))
    }

    async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, TableManagerError> {
        let tables: Option<Table> = sqlx::query(
            r#"
            SELECT
                share.name AS share_name,
                share.id::text AS share_id,
                "schema".name AS schema_name,
                "table".name AS table_name,
                "table".id::text AS table_id,
                "table".storage_path AS storage_path,
                "table".storage_format AS storage_format
            FROM share
            LEFT JOIN "schema" ON "schema".share_id = share.id
            LEFT JOIN "table" ON "table".schema_id = "schema".id
            WHERE share.name = $1 AND "schema".name = $2 AND "table".name = $3
            ORDER BY table_id
            "#,
        )
        .bind(share_name)
        .bind(schema_name)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| {
            let share_name = row.get("share_name");
            let share_id = row.get("share_id");
            let schema_name = row.get("schema_name");
            let table_name = row.get("table_name");
            let table_id = row.get("table_id");
            let storage_path = row.get("storage_path");
            let storage_format = row.get("storage_format");

            let share = Share::new(share_name, Some(share_id));
            let schema = Schema::new(share, schema_name);
            Table::new(
                schema,
                table_name,
                storage_path,
                Some(table_id),
                storage_format,
            )
        });

        match tables {
            Some(table) => Ok(table),
            None => Err(TableManagerError::TableNotFound {
                share_name: share_name.to_string(),
                schema_name: schema_name.to_string(),
                table_name: table_name.to_string(),
            }),
        }
    }
}

impl From<sqlx::Error> for TableManagerError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            // sqlx::Error::Configuration(_) => todo!(),
            // sqlx::Error::Database(_) => todo!(),
            // sqlx::Error::Io(_) => todo!(),
            // sqlx::Error::Tls(_) => todo!(),
            // sqlx::Error::Protocol(_) => todo!(),
            // sqlx::Error::RowNotFound => todo!(),
            // sqlx::Error::TypeNotFound { type_name } => todo!(),
            // sqlx::Error::ColumnIndexOutOfBounds { index, len } => todo!(),
            // sqlx::Error::ColumnNotFound(_) => todo!(),
            // sqlx::Error::ColumnDecode { index, source } => todo!(),
            // sqlx::Error::Decode(_) => todo!(),
            // sqlx::Error::PoolTimedOut => todo!(),
            // sqlx::Error::PoolClosed => todo!(),
            // sqlx::Error::WorkerCrashed => todo!(),
            // sqlx::Error::Migrate(_) => todo!(),
            _ => TableManagerError::Other {
                reason: err.to_string(),
            },
        }
    }
}
