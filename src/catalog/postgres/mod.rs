//! Catalog implementation leveraging Postgres as backing store.

use async_trait::async_trait;
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;

use crate::auth::RecipientId;

use self::{
    model::{
        RecipientModel, SchemaInfoModel, SchemaModel, ShareAclModel, ShareInfoModel, ShareModel,
        TableInfoModel, TableModel,
    },
    pagination::PostgresCursor,
};

use super::{Catalog, CatalogError, Page, Pagination, Schema, Share, Table};

mod model;
mod pagination;
/// Catalog implementation backed by a Postgres database.
#[derive(Debug, Clone)]
pub struct PostgresCatalog {
    pool: PgPool,
}

impl PostgresCatalog {
    /// Create a new PostgresCatalog.
    pub async fn new(connection_url: &str) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(500)
            .connect(connection_url)
            .await
            .expect("Failed to connect to Postgres");

        Self { pool }
    }

    /// Create a new PostgresCatalog from an existing PgPool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert a new client into the database.
    ///
    /// Clients are used to represent users or services that have access to the
    /// shared objects in the catalog. The client name is used to uniquely
    /// identify the client.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// use delta_sharing_server::catalog::postgres::PostgresCatalog;
    /// use delta_sharing_server::auth::ClientId;
    ///
    /// let catalog = PostgresCatalog::new("postgres://postgres:password@localhost:5432").await;
    /// let client_id = ClientId::known("foo");
    ///
    /// let client = catalog.insert_client(&client_id).await.unwrap();
    /// assert_eq!(client.name, "foo");
    /// # Ok::<(), Box<dyn std::error::Error>> };
    /// # Ok(()) }
    pub async fn insert_recipient(&self, recipient_name: &str) -> Result<RecipientModel, sqlx::Error> {
        let client = sqlx::query_as("INSERT INTO client (name) VALUES ($1) RETURNING *;")
            .bind(recipient_name)
            .fetch_one(&self.pool)
            .await?;

        Ok(client)
    }

    /// Select a client by name.
    ///
    /// Clients are used to represent users or services that have access to the
    /// shared objects in the catalog. The [`ClientId`] is used to uniquely identify
    /// the client. The client name is also unique accross all clients. The
    /// function returns `None` if no client with the given name exists.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let res = async {
    /// use delta_sharing_server::catalog::postgres::PostgresCatalog;
    /// use delta_sharing_server::auth::ClientId;
    ///
    /// let catalog = PostgresCatalog::new("postgres://postgres:password@localhost:5432").await;
    /// let client_id = ClientId::known("foo");
    ///
    /// let client = catalog.select_client_by_name(&client_id).await.unwrap();
    /// assert_eq!(client.unwrap().name, "foo");
    /// # Ok::<(), Box<dyn std::error::Error>> };
    /// # Ok(()) }
    pub async fn select_recipient_by_name(
        &self,
        recipient_name: &str,
    ) -> Result<Option<RecipientModel>, sqlx::Error> {
        let client = sqlx::query_as(
            r#"
            SELECT
                id,
                name
            FROM client
            WHERE name = $1;
            "#,
        )
        .bind(recipient_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(client)
    }

    /// Delete a client from the database.
    ///
    /// Clients are used to represent users or services that have access to the
    /// shared objects in the catalog. The [`ClientId`] is used to uniquely identify
    /// the client.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// use delta_sharing_server::catalog::postgres::PostgresCatalog;
    /// use delta_sharing_server::auth::ClientId;
    ///
    /// let catalog = PostgresCatalog::new("postgres://postgres:password@localhost:5432").await;
    /// let client_id = ClientId::known("foo");
    ///
    /// let client = catalog.select_client_by_name(&client_id).await.unwrap();
    /// let result = catalog.delete_client(&client.unwrap().id).await;
    /// assert!(result.is_ok());
    /// # Ok::<(), Box<dyn std::error::Error>> };
    /// # Ok(()) }
    pub async fn delete_client(&self, id: &Uuid) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM client WHERE id = $1;")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Insert a new share into the database.
    ///
    /// Shares are used to represent a collection of schemas and tables that are
    /// shared between clients. The share name is used to uniquely identify the
    /// share. The function returns a [`ShareModel`] object representing the
    /// newly created share.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// use delta_sharing_server::catalog::postgres::PostgresCatalog;
    ///
    /// let catalog = PostgresCatalog::new("postgres://postgres:password@localhost:5432").await;
    ///
    /// let share = catalog.insert_share("foo").await.unwrap();
    /// assert_eq!(share.name, "foo");
    /// # Ok::<(), Box<dyn std::error::Error>> };
    /// # Ok(()) }
    pub async fn insert_share(&self, share_name: &str) -> Result<ShareModel, sqlx::Error> {
        let share = sqlx::query_as("INSERT INTO share (name) VALUES ($1) RETURNING *;")
            .bind(share_name)
            .fetch_one(&self.pool)
            .await?;

        Ok(share)
    }

    /// Select a share by name.
    ///
    /// Shares are used to represent a collection of schemas and tables that are
    /// shared between clients. The share name is used to uniquely identify the
    /// share. The function returns `None` if no share with the given name exists.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// use delta_sharing_server::catalog::postgres::PostgresCatalog;
    /// use delta_sharing_server::auth::ClientId;
    ///
    /// let catalog = PostgresCatalog::new("postgres://postgres:password@localhost:5432").await;
    /// let client = ClientId::known("foo");
    ///
    /// let share = catalog.select_share_by_name(&client, "foo").await.unwrap();
    /// assert!(share.is_some());
    /// assert_eq!(share.unwrap().name, "foo");
    /// # Ok::<(), Box<dyn std::error::Error>> };
    /// # Ok(()) }
    pub async fn select_share_by_name(
        &self,
        recipient_name: &str,
        share_name: &str,
    ) -> Result<Option<ShareInfoModel>, sqlx::Error> {
        let share = sqlx::query_as(
            r#"
        WITH acl AS (
            SELECT
                s.share_id
            FROM client c
            JOIN share_acl s ON s.client_id = c.id
            WHERE c.name = $1
        )
        SELECT
            s.id,
            s.name
        FROM share s
        JOIN acl ON acl.share_id = s.id
        WHERE s.name = $2;
        "#,
        )
        .bind(recipient_name)
        .bind(share_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(share)
    }

    /// Select all shares that a client has access to.
    ///
    /// Shares are used to represent a collection of schemas and tables that are
    /// shared between clients. The share name is used to uniquely identify the
    /// share. The function returns a list of [`ShareInfoModel`] objects representing
    /// the shares that the client has access to.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// use delta_sharing_server::catalog::postgres::{PostgresCatalog, PostgresCursor};
    /// use delta_sharing_server::auth::ClientId;
    ///
    /// let catalog = PostgresCatalog::new("postgres://postgres:password@localhost:5432").await;
    /// let client = ClientId::known("foo");
    ///
    /// let shares = catalog.select_shares(&client, &PostgresCursor::default()).await.unwrap();
    /// assert_eq!(shares.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>> };
    /// # Ok(()) }
    pub async fn select_shares(
        &self,
        recipient_name: &str,
        cursor: &PostgresCursor,
    ) -> Result<Vec<ShareInfoModel>, sqlx::Error> {
        let shares = sqlx::query_as(
            r#"
            WITH acl AS (
                SELECT
                    s.share_id
                FROM client c
                JOIN share_acl s ON s.client_id = c.id
                WHERE c.name = $1
            )
            SELECT
                s.id,
                s.name
            FROM share s
            JOIN acl ON acl.share_id = s.id
            WHERE s.id > $2
            ORDER BY s.id ASC
            LIMIT $3;
            "#,
        )
        .bind(recipient_name)
        .bind(cursor.last_seen_id())
        .bind(cursor.limit())
        .fetch_all(&self.pool)
        .await?;

        Ok(shares)
    }

    /// Delete a share from the database.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// use delta_sharing_server::catalog::postgres::PostgresCatalog;
    /// use delta_sharing_server::auth::ClientId;
    ///
    /// let catalog = PostgresCatalog::new("postgres://postgres:password@localhost:5432").await;
    /// let client_id = ClientId::known("foo");
    ///
    /// let share = catalog.select_share_by_name(&client_id, "bar").await.unwrap().unwrap();
    /// let result = catalog.delete_share(&share.id).await;
    /// assert!(result.is_ok());
    /// # Ok::<(), Box<dyn std::error::Error>> };
    /// # Ok(()) }
    pub async fn delete_share(&self, id: &Uuid) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM share WHERE id = $1;")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Grant a client access to a share
    ///
    /// By default clients have no access to any shares. This function grants a
    /// client access to a share. The function returns a [`ShareAclModel`] object
    /// representing the newly created access control list entry.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// use delta_sharing_server::catalog::postgres::PostgresCatalog;
    /// use delta_sharing_server::auth::ClientId;
    ///
    /// let catalog = PostgresCatalog::new("postgres://postgres:password@localhost:5432").await;
    /// let client_id = ClientId::known("foo");
    /// let client = catalog.insert_client(&client_id).await.unwrap();
    /// let share = catalog.insert_share("bar").await.unwrap();
    ///
    /// let acl = catalog.grant_access_to_share(&client.id, &share.id).await.unwrap();
    /// assert_eq!(acl.client_id, client.id);
    /// assert_eq!(acl.share_id, share.id);
    /// # Ok::<(), Box<dyn std::error::Error>> };
    /// # Ok(()) }
    pub async fn grant_access_to_share(
        &self,
        client_id: &Uuid,
        share_id: &Uuid,
    ) -> Result<ShareAclModel, sqlx::Error> {
        let acl = sqlx::query_as(
            r#"
            INSERT INTO share_acl (client_id, share_id)
            VALUES ($1, $2)
            RETURNING *;
            "#,
        )
        .bind(client_id)
        .bind(share_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(acl)
    }

    /// Revoke a client's access to a share
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// use delta_sharing_server::catalog::postgres::PostgresCatalog;
    /// use delta_sharing_server::auth::ClientId;
    ///
    /// let catalog = PostgresCatalog::new("postgres://postgres:password@localhost:5432").await;
    /// let client_id = ClientId::known("foo");
    /// let client = catalog.select_client_by_name(&client_id).await.unwrap().unwrap();
    /// let share = catalog.select_share_by_name(&client_id, "bar").await.unwrap().unwrap();
    ///
    /// let result = catalog.revoke_access_to_share(&client.id, &share.id).await;
    /// assert!(result.is_ok());
    /// # Ok::<(), Box<dyn std::error::Error>> };
    /// # Ok(()) }
    pub async fn revoke_access_to_share(
        &self,
        client_id: &Uuid,
        share_id: &Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            DELETE FROM share_acl
            WHERE client_id = $1 AND share_id = $2;
            "#,
        )
        .bind(client_id)
        .bind(share_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Insert a new schema into the database.
    pub async fn insert_schema(
        &self,
        share_id: &Uuid,
        schema_name: &str,
    ) -> Result<SchemaModel, sqlx::Error> {
        let schema = sqlx::query_as(
            r#"
            INSERT INTO schema (name, share_id)
            VALUES ($1, $2) 
            RETURNING *;
            "#,
        )
        .bind(schema_name)
        .bind(share_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(schema)
    }

    /// Select a schema by name.
    pub async fn select_schema_by_name(
        &self,
        client_id: &str,
        share_name: &str,
        schema_name: &str,
    ) -> Result<Option<SchemaInfoModel>, sqlx::Error> {
        let schema: Option<SchemaInfoModel> = sqlx::query_as(
            r#"
            WITH acl AS (
                SELECT
                    sh.share_id
                FROM client c
                JOIN share_acl sh ON sh.client_id = c.id
                WHERE c.name = $1
            )
            SELECT
                sc.id,
                sc.name,
                sh.name AS share_name
            FROM schema sc
            JOIN share sh ON sh.id = sc.share_id
            JOIN acl ON acl.share_id = sh.id
            WHERE sh.name = $2 AND sc.name = $3;
            "#,
        )
        .bind(client_id)
        .bind(share_name)
        .bind(schema_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(schema)
    }

    /// Select all schemas within a shares that a client has access to.
    pub async fn select_schemas(
        &self,
        recipient_name: &str,
        share_name: &str,
        cursor: &PostgresCursor,
    ) -> Result<Vec<SchemaInfoModel>, sqlx::Error> {
        let schemas = sqlx::query_as(
            r#"
            WITH acl AS (
                SELECT
                    sh.share_id
                FROM client c
                JOIN share_acl sh ON sh.client_id = c.id
                WHERE c.name = $1
            )
            SELECT
                sc.id,
                sc.name,
                sh.name AS share_name
            FROM schema sc
            JOIN share sh ON sh.id = sc.share_id
            JOIN acl ON acl.share_id = sh.id
            WHERE sh.name = $2 AND sc.id > $3
            ORDER BY sc.id ASC
            LIMIT $4;
            "#,
        )
        .bind(recipient_name)
        .bind(share_name)
        .bind(cursor.last_seen_id())
        .bind(cursor.limit())
        .fetch_all(&self.pool)
        .await?;

        Ok(schemas)
    }

    /// Delete a schema from the database.
    pub async fn delete_schema(&self, id: &Uuid) -> Result<(), sqlx::Error> {
        sqlx::query(r#"DELETE FROM "schema" WHERE id = $1;"#)
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Insert a new table into the database.
    pub async fn insert_table(
        &self,
        schema_id: &Uuid,
        table_name: &str,
        storage_path: &str,
    ) -> Result<TableModel, sqlx::Error> {
        let table = sqlx::query_as(
            r#"
            INSERT INTO "table" (name, schema_id, storage_path) 
            VALUES ($1, $2, $3)
            RETURNING *;
            "#,
        )
        .bind(table_name)
        .bind(schema_id)
        .bind(storage_path)
        .fetch_one(&self.pool)
        .await?;

        Ok(table)
    }

    /// Select a table by name.
    pub async fn select_table_by_name(
        &self,
        recipient_name: &str,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfoModel>, sqlx::Error> {
        let table = sqlx::query_as(
            r#"
            WITH acl AS (
                SELECT
                    sh.share_id
                FROM client c
                JOIN share_acl sh ON sh.client_id = c.id
                WHERE c.name = $1
            )
            SELECT
                t.id,
                sh.id AS share_id,
                t.name,
                sc.name AS schema_name,
                sh.name AS share_name,
                t.storage_path
            FROM "table" t
            JOIN schema sc ON sc.id = t.schema_id
            JOIN share sh ON sh.id = sc.share_id
            JOIN acl ON acl.share_id = sh.id
            WHERE sh.name = $2 AND sc.name = $3 AND t.name = $4;
            "#,
        )
        .bind(recipient_name)
        .bind(share_name)
        .bind(schema_name)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(table)
    }

    /// Select all tables within a schema that a client has access to.
    pub async fn select_tables_by_schema(
        &self,
        recipient_name: &str,
        share_name: &str,
        schema_name: &str,
        cursor: &PostgresCursor,
    ) -> Result<Vec<TableInfoModel>, sqlx::Error> {
        let tables = sqlx::query_as(
            r#"
            WITH acl AS (
                SELECT
                    sh.share_id
                FROM client c
                JOIN share_acl sh ON sh.client_id = c.id
                WHERE c.name = $1
            )
            SELECT
                t.id,
                sh.id AS share_id,
                t.name,
                sc.name AS schema_name,
                sh.name AS share_name,
                t.storage_path
            FROM "table" t
            JOIN schema sc ON sc.id = t.schema_id
            JOIN share sh ON sh.id = sc.share_id
            JOIN acl ON acl.share_id = sh.id
            WHERE sh.name = $2 AND sc.name = $3 AND t.id > $4
            ORDER BY t.id ASC
            LIMIT $5;
            "#,
        )
        .bind(recipient_name)
        .bind(share_name)
        .bind(schema_name)
        .bind(cursor.last_seen_id())
        .bind(cursor.limit())
        .fetch_all(&self.pool)
        .await?;

        Ok(tables)
    }

    /// Select all tables within a share that a client has access to.
    pub async fn select_tables_by_share(
        &self,
        recipient_name: &str,
        share_name: &str,
        cursor: &PostgresCursor,
    ) -> Result<Vec<TableInfoModel>, sqlx::Error> {
        let tables = sqlx::query_as(
            r#"
            WITH acl AS (
                SELECT
                    sh.share_id
                FROM client c
                JOIN share_acl sh ON sh.client_id = c.id
                WHERE c.name = $1
            )
            SELECT
                t.id,
                sh.id AS share_id,
                t.name,
                sc.name AS schema_name,
                sh.name AS share_name,
                t.storage_path
            FROM "table" t
            JOIN schema sc ON sc.id = t.schema_id
            JOIN share sh ON sh.id = sc.share_id
            JOIN acl ON acl.share_id = sh.id
            WHERE sh.name = $2 AND t.id > $3
            ORDER BY t.id ASC
            LIMIT $4;
            "#,
        )
        .bind(recipient_name)
        .bind(share_name)
        .bind(cursor.last_seen_id())
        .bind(cursor.limit())
        .fetch_all(&self.pool)
        .await?;

        Ok(tables)
    }

    /// Delete a table from the database.
    pub async fn delete_table(&self, id: &Uuid) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            DELETE FROM "table"
            WHERE id = $1;
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl Catalog for PostgresCatalog {
    async fn list_shares(
        &self,
        client_id: &RecipientId,
        cursor: &Pagination,
    ) -> Result<Page<Share>, CatalogError> {
        let pg_cursor = PostgresCursor::try_from(cursor.clone())
            .map_err(|e| CatalogError::malformed_pagination(e.to_string()))?;
        let shares_info_models = self.select_shares(client_id, &pg_cursor).await?;

        let shares = shares_info_models
            .into_iter()
            .map(|s| s.try_into_share())
            .collect::<Result<Vec<_>, _>>()?;
        let next_page_token = shares
            .get(pg_cursor.limit() as usize - 1)
            .and_then(|s| s.id().map(ToOwned::to_owned));

        Ok(Page::new(shares, next_page_token))
    }

    async fn list_schemas(
        &self,
        client_id: &RecipientId,
        share_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<Schema>, CatalogError> {
        let pg_cursor = PostgresCursor::try_from(cursor.clone())
            .map_err(|e| CatalogError::malformed_pagination(e.to_string()))?;
        let schemas_models = self
            .select_schemas(client_id, share_name, &pg_cursor)
            .await?;

        let schemas = schemas_models
            .into_iter()
            .map(|s| s.try_into_schema())
            .collect::<Result<Vec<_>, _>>()?;
        let next_page_token = schemas
            .get(pg_cursor.limit() as usize - 1)
            .and_then(|s| s.id().map(ToOwned::to_owned));

        Ok(Page::new(schemas, next_page_token))
    }

    async fn list_tables_in_share(
        &self,
        client_id: &RecipientId,
        share_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<Table>, CatalogError> {
        let pg_cursor = PostgresCursor::try_from(cursor.clone())
            .map_err(|e| CatalogError::malformed_pagination(e.to_string()))?;
        let table_models = self
            .select_tables_by_share(client_id, share_name, &pg_cursor)
            .await?;

        let tables = table_models
            .into_iter()
            .map(|t| t.try_into_table())
            .collect::<Result<Vec<_>, _>>()?;
        let next_page_token = tables
            .get(pg_cursor.limit() as usize - 1)
            .and_then(|s| s.id().map(ToOwned::to_owned));

        Ok(Page::new(tables, next_page_token))
    }

    async fn list_tables_in_schema(
        &self,
        client_id: &RecipientId,
        share_name: &str,
        schema_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<Table>, CatalogError> {
        let pg_cursor = PostgresCursor::try_from(cursor.clone())
            .map_err(|e| CatalogError::malformed_pagination(e.to_string()))?;
        let table_models = self
            .select_tables_by_schema(client_id, share_name, schema_name, &pg_cursor)
            .await?;

        let tables = table_models
            .into_iter()
            .map(|t| t.try_into_table())
            .collect::<Result<Vec<_>, _>>()?;
        let next_page_token = tables
            .get(pg_cursor.limit() as usize - 1)
            .and_then(|s| s.id().map(ToOwned::to_owned));

        Ok(Page::new(tables, next_page_token))
    }

    async fn get_share(
        &self,
        client_id: &RecipientId,
        share_name: &str,
    ) -> Result<Share, CatalogError> {
        self.select_share_by_name(client_id, share_name)
            .await?
            .ok_or(CatalogError::not_found(format!(
                "share `{}` does not exist or is not accessible",
                share_name
            )))
            .and_then(|s| s.try_into_share())
    }

    async fn get_table(
        &self,
        client_id: &RecipientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, CatalogError> {
        self.select_table_by_name(client_id, share_name, schema_name, table_name)
            .await?
            .ok_or(CatalogError::not_found(format!(
                "table `{}.{}.{}` does not exist or is not accessible",
                share_name, schema_name, table_name
            )))
            .and_then(|t| t.try_into_table())
    }
}

impl From<sqlx::Error> for CatalogError {
    fn from(err: sqlx::Error) -> Self {
        CatalogError::internal(err.to_string())
    }
}
