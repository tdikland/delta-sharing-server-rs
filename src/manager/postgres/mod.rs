use std::convert::Infallible;

use async_trait::async_trait;
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    PgPool, Row,
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
        .collect()
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

        Ok(Schema::new(share.clone(), schema_name.to_string(), Some(schema_id.to_string())))
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
        .collect()
    }

    async fn insert_table(&self, schema: &Schema, table_name: &str, storage_path: &str, storage_format: Option<&String>) -> Result<Table, sqlx::Error> {
        let uuid = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO \"table\" (id, name, schema_id, storage_path, storage_format) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(uuid)
        .bind(table_name)
        .bind(schema.id().unwrap()) // TODO
        .bind(storage_path)
        .bind(storage_format)
        .execute(&self.pool)
        .await?;

        Ok(Table::new(schema.clone(), table_name.to_owned(), storage_path.to_owned(), Some(uuid.to_string()), storage_format.cloned()))
    }

    async fn select_tables_by_share(&self, share_name: &str, cursor: &PostgresCursor) -> Result<Vec<Table>, sqlx::Error> {
        sqlx::query(
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
            WHERE share.name = $1 AND "table".id > $2
            ORDER BY table_id
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
        .collect()
    }

    async fn select_tables_by_schema(&self, share_name: &str, schema_name: &str, cursor: &PostgresCursor) -> Result<Vec<Table>, sqlx::Error> {
        sqlx::query(
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
            WHERE share.name = $1 AND "schema".name = $2 AND "table".id > $3
            ORDER BY table_id
            LIMIT $4
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

    async fn select_table_by_name(&self, share_name: &str, schema_name: &str, table_name: &str) -> Result<Option<Table>, sqlx::Error> {
        sqlx::query(
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
        .map(TryFrom::try_from)
        .transpose()
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

impl TryFrom<ListCursor> for PostgresCursor {
    type Error = Infallible;
    // TODO: use Option::map, set limit to maxResults
    fn try_from(c: ListCursor) -> Result<Self, Self::Error> {
        let last_seen_id = match c.page_token() {
            Some(token) => Some(Uuid::parse_str(token).unwrap()),
            None => None,
        };
        let cursor = PostgresCursor::new(last_seen_id, None);
        Ok(cursor)
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
        let schema_id: String = row.try_get("schema_id")?;
        let schema_name: String = row.try_get("schema_name")?;
        let share = Share::new(share_name, Some(share_id));
        Ok(Schema::new(share, schema_name, Some(schema_id)))
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

        let share = Share::new(share_name, Some(share_id));
        let schema = Schema::new(share, schema_name, Some(schema_id));
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
    async fn list_shares(&self, cursor: &ListCursor) -> Result<List<Share>, TableManagerError> {
        let c: PostgresCursor = PostgresCursor::try_from(cursor.clone()).unwrap(); // TODO: fix unwrap
        let shares = self.select_shares(&c).await?;
        let last_seen_id = shares.iter().last().and_then(|s| {s.id().cloned()});
        Ok(List::new(shares, last_seen_id))
    }

    async fn get_share(&self, share_name: &str) -> Result<Share, TableManagerError> {
        self.select_share_by_name(share_name).await?

        // let share: Option<Share> = sqlx::query(
        //     r#"
        //     SELECT name, id::text
        //     FROM share
        //     WHERE name = $1
        //     "#,
        // )
        // .bind(share_name)
        // .fetch_optional(&self.pool)
        // .await?
        // .map(|row| {
        //     let name: String = row.get(0);
        //     let id: String = row.get(1);
        //     Share::new(name, Some(id))
        // });

        // match share {
        //     Some(share) => Ok(share),
        //     None => Err(TableManagerError::ShareNotFound {
        //         share_name: share_name.to_string(),
        //     }),
        // }
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
                    Schema::new(share, schema_name, None)
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
                    let schema = Schema::new(share, schema_name, None);
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
                    let schema = Schema::new(share, schema_name, None);
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
            let schema = Schema::new(share, schema_name, None);
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
