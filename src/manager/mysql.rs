use async_trait::async_trait;
use sqlx::mysql::{MySqlPoolOptions, MySqlRow};
use sqlx::MySqlPool;
use sqlx::Row;

#[derive(Debug)]
pub struct MySqlTableManager {
    pool: MySqlPool,
}

use crate::protocol::securables::{Schema, Share, Table};

use super::{List, ListCursor, TableManager, TableManagerError};

impl MySqlTableManager {
    pub async fn new(connection_url: &str) -> Self {
        let pool = MySqlPoolOptions::new()
            .max_connections(25)
            .connect(connection_url)
            .await
            .expect("failed to connect to mysql");

        Self { pool }
    }

    pub fn from_pool(pool: MySqlPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    pub async fn insert_share(&self, share_name: &str) -> Result<Share, sqlx::Error> {
        let insert = sqlx::query("INSERT INTO share (name) VALUES (?);")
            .bind(share_name)
            .execute(&self.pool)
            .await?;
        let share_id = insert.last_insert_id().to_string();

        Ok(Share::new(share_name.to_string(), Some(share_id)))
    }

    async fn select_share_by_name(&self, share_name: &str) -> Result<Option<Share>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 
                id AS share_id,
                name AS share_name
            FROM share
            WHERE name = ?;
            "#,
        )
        .bind(share_name)
        .fetch_optional(&self.pool)
        .await?
        .map(TryFrom::try_from)
        .transpose()
    }

    async fn select_shares(&self, cursor: &MySqlCursor) -> Result<Vec<Share>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 
                id AS share_id,
                name AS share_name
            FROM share
            WHERE id > ?
            ORDER BY id
            LIMIT ?;
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

    pub async fn delete_shares(&self) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM share;")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn insert_schema(
        &self,
        share: &Share,
        schema_name: &str,
    ) -> Result<Schema, sqlx::Error> {
        let insert = sqlx::query(
            r#"
            INSERT INTO `schema` (name, share_id) 
            VALUES (?, ?);
            "#,
        )
        .bind(schema_name)
        .bind(share.id().unwrap())
        .execute(&self.pool)
        .await?;
        let schema_id = insert.last_insert_id().to_string();

        Ok(Schema::new(
            share.clone(),
            schema_name.to_string(),
            Some(schema_id),
        ))
    }

    async fn select_schema_by_name(
        &self,
        share_name: &str,
        schema_name: &str,
    ) -> Result<Option<Schema>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 
                share.id AS share_id,
                share.name AS share_name,
                `schema`.id AS schema_id,
                `schema`.name AS schema_name
            FROM share
            LEFT JOIN `schema` ON `schema`.share_id = share.id
            WHERE share.name = ? AND `schema`.name = ?;
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
        cursor: &MySqlCursor,
    ) -> Result<Vec<Schema>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 
                share.id AS share_id,
                share.name AS share_name,
                `schema`.id AS schema_id,
                `schema`.name AS schema_name
            FROM share
            LEFT JOIN `schema` ON `schema`.share_id = share.id
            WHERE share.name = ? AND `schema`.id > ?
            ORDER BY `schema`.id
            LIMIT ?;
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

    pub async fn delete_schemas(&self) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM `schema`;")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn insert_table(
        &self,
        schema: &Schema,
        table_name: &str,
        storage_path: &str,
        storage_format: Option<&String>,
    ) -> Result<Table, sqlx::Error> {
        let insert = sqlx::query(
            r#"
            INSERT INTO `table` (name, schema_id, storage_path, storage_format) 
            VALUES (?, ?, ?, ?);
            "#,
        )
        .bind(table_name)
        .bind(schema.id().unwrap())
        .bind(storage_path)
        .bind(storage_format)
        .execute(&self.pool)
        .await?;
        let table_id = insert.last_insert_id().to_string();

        Ok(Table::new(
            schema.clone(),
            table_name.to_owned(),
            storage_path.to_owned(),
            Some(table_id),
            storage_format.cloned(),
        ))
    }

    async fn select_tables_by_share(
        &self,
        share_name: &str,
        cursor: &MySqlCursor,
    ) -> Result<Vec<Table>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT
                share.id AS share_id,
                share.name AS share_name,
                `schema`.id AS schema_id,
                `schema`.name AS schema_name,
                `table`.id AS table_id,
                `table`.name AS table_name,
                `table`.storage_path AS storage_path,
                `table`.storage_format AS storage_format
            FROM share
            LEFT JOIN `schema` ON `schema`.share_id = share.id
            LEFT JOIN `table` ON `table`.schema_id = `schema`.id
            WHERE share.name = ? AND `table`.id > ?
            ORDER BY `table`.id
            LIMIT ?;
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
        cursor: &MySqlCursor,
    ) -> Result<Vec<Table>, sqlx::Error> {
        sqlx::query(
            r#"
            SELECT
                share.id AS share_id,
                share.name AS share_name,
                `schema`.id AS schema_id,
                `schema`.name AS schema_name,
                `table`.id AS table_id,
                `table`.name AS table_name,
                `table`.storage_path AS storage_path,
                `table`.storage_format AS storage_format
            FROM share
            LEFT JOIN `schema` ON `schema`.share_id = share.id
            LEFT JOIN `table` ON `table`.schema_id = `schema`.id
            WHERE share.name = ? AND `schema`.name = ? AND `table`.id > ?
            ORDER BY `table`.id
            LIMIT ?;
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
                share.id AS share_id,
                share.name AS share_name,
                `schema`.id AS schema_id,
                `schema`.name AS schema_name,
                `table`.id AS table_id,
                `table`.name AS table_name,
                `table`.storage_path AS storage_path,
                `table`.storage_format AS storage_format
            FROM share
            LEFT JOIN `schema` ON `schema`.share_id = share.id
            LEFT JOIN `table` ON `table`.schema_id = `schema`.id
            WHERE share.name = ? AND `schema`.name = ? AND `table`.name = ?;
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

    pub async fn delete_tables(&self) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM `table`;")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
struct MySqlCursor {
    last_seen_id: Option<u64>,
    limit: Option<u32>,
}

impl MySqlCursor {
    pub fn new(last_seen_id: Option<u64>, limit: Option<u32>) -> Self {
        Self {
            last_seen_id,
            limit,
        }
    }

    pub fn last_seen_id(&self) -> u64 {
        match self.last_seen_id {
            Some(id) => id,
            None => 0,
        }
    }

    pub fn limit(&self) -> i32 {
        match self.limit {
            Some(lim) => lim as i32,
            None => 100,
        }
    }
}

use core::str::FromStr;

impl TryFrom<ListCursor> for MySqlCursor {
    type Error = &'static str;
    fn try_from(cursor: ListCursor) -> Result<Self, Self::Error> {
        let last_seen_id = cursor
            .page_token()
            .map(|token| u64::from_str(token).map_err(|_| "invalid page token"))
            .transpose()?;
        let pg_cursor = MySqlCursor::new(last_seen_id, cursor.max_results());
        Ok(pg_cursor)
    }
}

impl TryFrom<MySqlRow> for Share {
    type Error = sqlx::Error;

    fn try_from(row: MySqlRow) -> Result<Self, Self::Error> {
        let name: String = row.try_get("share_name")?;
        let id: i32 = row.try_get("share_id")?;
        Ok(Share::new(name, Some(id.to_string())))
    }
}

impl TryFrom<MySqlRow> for Schema {
    type Error = sqlx::Error;

    fn try_from(row: MySqlRow) -> Result<Self, Self::Error> {
        let share_id: i32 = row.try_get("share_id")?;
        let share_name: String = row.try_get("share_name")?;
        let schema_id: i32 = row.try_get("schema_id")?;
        let schema_name: String = row.try_get("schema_name")?;
        let share = Share::new(share_name, Some(share_id.to_string()));
        Ok(Schema::new(share, schema_name, Some(schema_id.to_string())))
    }
}

impl TryFrom<MySqlRow> for Table {
    type Error = sqlx::Error;

    fn try_from(row: MySqlRow) -> Result<Self, Self::Error> {
        let share_id: i32 = row.try_get("share_id")?;
        let share_name: String = row.try_get("share_name")?;
        let schema_id: i32 = row.try_get("schema_id")?;
        let schema_name: String = row.try_get("schema_name")?;
        let table_id: i32 = row.try_get("table_id")?;
        let table_name: String = row.try_get("table_name")?;
        let storage_path: String = row.try_get("storage_path")?;
        let storage_format: Option<String> = row.try_get("storage_format")?;

        let share = Share::new(share_name, Some(share_id.to_string()));
        let schema = Schema::new(share, schema_name, Some(schema_id.to_string()));
        Ok(Table::new(
            schema,
            table_name,
            storage_path,
            Some(table_id.to_string()),
            storage_format,
        ))
    }
}

#[async_trait]
impl TableManager for MySqlTableManager {
    async fn list_shares(&self, cursor: &ListCursor) -> Result<List<Share>, TableManagerError> {
        let pg_cursor = MySqlCursor::try_from(cursor.clone())
            .map_err(|_| TableManagerError::MalformedContinuationToken)?;
        let shares = self.select_shares(&pg_cursor).await?;

        let next_page_token = if shares.len() == pg_cursor.limit() as usize {
            shares.iter().last().and_then(|s| s.id().cloned())
        } else {
            None
        };

        Ok(List::new(shares, next_page_token))
    }

    async fn get_share(&self, share_name: &str) -> Result<Share, TableManagerError> {
        self.select_share_by_name(share_name)
            .await?
            .ok_or(TableManagerError::ShareNotFound {
                share_name: share_name.to_string(),
            })
    }

    async fn list_schemas(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Schema>, TableManagerError> {
        let pg_cursor = MySqlCursor::try_from(cursor.clone())
            .map_err(|_| TableManagerError::MalformedContinuationToken)?;
        let schemas = self
            .select_schemas_by_share_name(share_name, &pg_cursor)
            .await?;

        let next_page_token = if schemas.len() == pg_cursor.limit() as usize {
            schemas.iter().last().and_then(|s| s.id().cloned())
        } else {
            None
        };

        Ok(List::new(schemas, next_page_token))
    }

    async fn list_tables_in_share(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Table>, TableManagerError> {
        let pg_cursor = MySqlCursor::try_from(cursor.clone())
            .map_err(|_| TableManagerError::MalformedContinuationToken)?;
        let tables = self.select_tables_by_share(share_name, &pg_cursor).await?;

        let next_page_token = if tables.len() == pg_cursor.limit() as usize {
            tables.iter().last().and_then(|s| s.table_id().cloned())
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
    ) -> Result<List<Table>, TableManagerError> {
        let pg_cursor = MySqlCursor::try_from(cursor.clone())
            .map_err(|_| TableManagerError::MalformedContinuationToken)?;
        let tables = self
            .select_tables_by_schema(share_name, schema_name, &pg_cursor)
            .await?;

        let next_page_token = if tables.len() == pg_cursor.limit() as usize {
            tables.iter().last().and_then(|s| s.table_id().cloned())
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
    ) -> Result<Table, TableManagerError> {
        match self
            .select_table_by_name(share_name, schema_name, table_name)
            .await
        {
            Ok(Some(table)) => Ok(table),
            Ok(None) => {
                let share = self.select_share_by_name(share_name).await?;
                let schema = self.select_schema_by_name(share_name, schema_name).await?;
                match (share, schema) {
                    (None, _) => Err(TableManagerError::ShareNotFound {
                        share_name: share_name.to_owned(),
                    }),
                    (Some(_), None) => Err(TableManagerError::SchemaNotFound {
                        share_name: share_name.to_owned(),
                        schema_name: schema_name.to_owned(),
                    }),
                    (Some(_), Some(_)) => Err(TableManagerError::TableNotFound {
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
// impl From<sqlx::Error> for TableManagerError {
//     fn from(err: sqlx::Error) -> Self {
//         match err {
//             _ => TableManagerError::Other {
//                 reason: err.to_string(),
//             },
//         }
//     }
// }
