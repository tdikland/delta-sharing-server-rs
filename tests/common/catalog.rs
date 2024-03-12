use delta_sharing_server::{auth::ClientId, catalog::postgres::PostgresCatalog};
use sqlx::PgPool;
use testcontainers::{clients::Cli, Container};
use testcontainers_modules::postgres::Postgres;

struct PostgresCatalogTestContext<'a> {
    docker: &'a Cli,
    container: Container<'a, Postgres>,
    catalog: PostgresCatalog,
}

impl<'a> PostgresCatalogTestContext<'a> {
    async fn new(docker: &'a Cli) -> Self {
        // Start container
        let container = docker.run(Postgres::default());

        // Build connection pool
        let url = format!(
            "postgres://postgres:postgres@127.0.0.1:{}/postgres",
            container.get_host_port_ipv4(5432)
        );
        let pool = PgPool::connect(&url)
            .await
            .expect("Failed to connect to Postgres");

        // Run migrations
        sqlx::migrate!("tests/migrations/postgres")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        // Build catalog implementation
        let catalog = PostgresCatalog::from_pool(pool);

        Self {
            docker,
            container,
            catalog,
        }
    }

    async fn seed(&self) -> Result<(), Box<dyn std::error::Error>> {
        let c = &self.catalog;

        let anon_id = ClientId::anonymous();
        let known_id = ClientId::known("client1");

        let anon = c.insert_client(&anon_id).await?;
        let known = c.insert_client(&known_id).await?;

        // Insert shares
        let share1 = c.insert_share("share1").await?;
        let share2 = c.insert_share("share2").await?;
        let share3 = c.insert_share("share3").await?;

        // Mark the first two shares as publicly shared
        c.grant_access_to_share(&anon.id, &share1.id).await?;
        c.grant_access_to_share(&anon.id, &share2.id).await?;

        // Mark the last share as privately shared to client1
        c.grant_access_to_share(&known.id, &share3.id).await?;

        // Insert schemas
        let schema1 = c.insert_schema(&share1.id, "schema1").await?;
        let schema2 = c.insert_schema(&share1.id, "schema2").await?;

        c.grant_access_to_schema(&anon.id, &schema1.id).await?;
        c.grant_access_to_schema(&anon.id, &schema2.id).await?;

        // Insert tables
        let table1 = c.insert_table(&schema1.id, "table1", "p1").await?;
        let table2 = c.insert_table(&schema1.id, "table2", "p2").await?;
        let table3 = c.insert_table(&schema2.id, "table3", "p3").await?;

        // Grant access to tables
        c.grant_access_to_table(&anon.id, &table1.id).await?;
        c.grant_access_to_table(&anon.id, &table2.id).await?;
        c.grant_access_to_table(&anon.id, &table3.id).await?;

        Ok(())
    }
}
