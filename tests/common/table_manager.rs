use delta_sharing_server_rs::manager::{mysql::MySqlTableManager, postgres::PostgresTableManager};
use sqlx::{Connection, Executor, MySqlConnection, PgConnection};
use std::env;
use url::Url;
use uuid::Uuid;

#[derive(Debug)]
pub struct IntegrationContext {
    manager: Manager,
    db_url: String,
    db_name: String,
}

impl IntegrationContext {
    pub async fn setup_postgres() -> Self {
        let db_url = env::var("POSTGRES_CONNECTION_URL").expect("`POSTGRES_CONNECTION_URL` is set");
        let db_name = format!("test_db_{}", Uuid::new_v4().as_u128());
        let mut connection = PgConnection::connect(&db_url)
            .await
            .expect("failed to connect to postgres");

        connection
            .execute(format!("CREATE DATABASE {};", db_name).as_str())
            .await
            .expect("failed to create database");

        let mut connection_url = Url::parse(&db_url).unwrap();
        connection_url.set_path(&db_name);
        let pg_manager = PostgresTableManager::new(connection_url.as_str()).await;

        let mut manager = Manager::Postgres(pg_manager);
        manager.setup().await;
        manager.prepare_data().await;

        Self {
            manager,
            db_url: db_url.to_string(),
            db_name: db_name.to_string(),
        }
    }

    pub async fn setup_mysql() -> Self {
        let db_url = env::var("MYSQL_CONNECTION_URL").expect("`MYSQL_CONNECTION_URL` is set");
        let db_name = format!("test_db_{}", Uuid::new_v4().as_u128());
        dbg!(&db_name);
        let mut connection = MySqlConnection::connect(&db_url)
            .await
            .expect("failed to connect to mysql");

        connection
            .execute(format!("CREATE DATABASE {};", db_name).as_str())
            .await
            .expect("failed to create database");

        let mut connection_url = Url::parse(&db_url).unwrap();
        connection_url.set_path(&db_name);
        let mysql_manager = MySqlTableManager::new(connection_url.as_str()).await;

        let mut manager = Manager::MySql(mysql_manager);
        manager.setup().await;
        manager.prepare_data().await;

        Self {
            manager,
            db_url: db_url.to_string(),
            db_name: db_name.to_string(),
        }
    }

    async fn teardown(&mut self) {
        self.manager.teardown(&self.db_url, &self.db_name).await
    }

    pub fn as_pg(&self) -> &PostgresTableManager {
        if let Manager::Postgres(pg) = &self.manager {
            pg
        } else {
            panic!("expected pg table manager")
        }
    }

    pub fn as_mysql(&self) -> &MySqlTableManager {
        if let Manager::MySql(mysql) = &self.manager {
            mysql
        } else {
            panic!("expected mysql table manager")
        }
    }
}

impl Drop for IntegrationContext {
    fn drop(&mut self) {
        std::thread::scope(|s| {
            s.spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(self.teardown());
            });
        });
    }
}

#[derive(Debug)]
pub enum Manager {
    Postgres(PostgresTableManager),
    MySql(MySqlTableManager),
}

impl Manager {
    pub async fn setup(&mut self) {
        match self {
            Manager::Postgres(pg) => {
                sqlx::migrate!("tests/migrations/postgres")
                    .run(pg.pool())
                    .await
                    .unwrap();
            }
            Manager::MySql(mysql) => {
                sqlx::migrate!("tests/migrations/mysql")
                    .run(mysql.pool())
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn prepare_data(&mut self) {
        match self {
            Manager::Postgres(pg) => {
                let share1 = pg.insert_share("share_1").await.unwrap();
                let share2 = pg.insert_share("share_2").await.unwrap();
                let _share3 = pg.insert_share("share_3").await.unwrap();

                let schema_11 = pg.insert_schema(&share1, "schema_1").await.unwrap();
                let schema_12 = pg.insert_schema(&share1, "schema_2").await.unwrap();
                let _schema_21 = pg.insert_schema(&share2, "schema_1").await.unwrap();

                let _table_111 = pg
                    .insert_table(
                        &schema_11,
                        "table_1",
                        &format!("s3://bucket/table_111/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_112 = pg
                    .insert_table(
                        &schema_11,
                        "table_2",
                        &format!("s3://bucket/table_112/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_113 = pg
                    .insert_table(
                        &schema_11,
                        "table_3",
                        &format!("s3://bucket/table_113/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_114 = pg
                    .insert_table(
                        &schema_11,
                        "table_4",
                        &format!("s3://bucket/table_114/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_121 = pg
                    .insert_table(
                        &schema_12,
                        "table_1",
                        &format!("s3://bucket/table_121/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_122 = pg
                    .insert_table(
                        &schema_12,
                        "table_2",
                        &format!("s3://bucket/table_122/"),
                        None,
                    )
                    .await
                    .unwrap();
            }
            Manager::MySql(mysql) => {
                let share1 = mysql.insert_share("share_1").await.unwrap();
                let share2 = mysql.insert_share("share_2").await.unwrap();
                let _share3 = mysql.insert_share("share_3").await.unwrap();

                let schema_11 = mysql.insert_schema(&share1, "schema_1").await.unwrap();
                let schema_12 = mysql.insert_schema(&share1, "schema_2").await.unwrap();
                let _schema_21 = mysql.insert_schema(&share2, "schema_1").await.unwrap();

                let _table_111 = mysql
                    .insert_table(
                        &schema_11,
                        "table_1",
                        &format!("s3://bucket/table_111/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_112 = mysql
                    .insert_table(
                        &schema_11,
                        "table_2",
                        &format!("s3://bucket/table_112/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_113 = mysql
                    .insert_table(
                        &schema_11,
                        "table_3",
                        &format!("s3://bucket/table_113/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_114 = mysql
                    .insert_table(
                        &schema_11,
                        "table_4",
                        &format!("s3://bucket/table_114/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_121 = mysql
                    .insert_table(
                        &schema_12,
                        "table_1",
                        &format!("s3://bucket/table_121/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_122 = mysql
                    .insert_table(
                        &schema_12,
                        "table_2",
                        &format!("s3://bucket/table_122/"),
                        None,
                    )
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn teardown(&mut self, db_url: &str, db_name: &str) {
        match self {
            Manager::Postgres(pg) => {
                pg.pool().close().await;
                let mut connection = PgConnection::connect(db_url)
                    .await
                    .expect("Failed to connect to Postgres");

                // Force drop all active connections to database
                connection
                    .execute(
                        format!(
                            r#"
                                SELECT pg_terminate_backend(pg_stat_activity.pid)
                                FROM pg_stat_activity
                                WHERE pg_stat_activity.datname = '{}'
                                AND pid <> pg_backend_pid()
                                "#,
                            db_name
                        )
                        .as_str(),
                    )
                    .await
                    .expect("Failed to terminate current connections to test db");

                connection
                    .execute(format!(r#"DROP DATABASE "{}";"#, db_name).as_str())
                    .await
                    .expect("Failed to drop database.");
            }
            Manager::MySql(mysql) => {
                mysql.pool().close().await;
                let mut connection = MySqlConnection::connect(db_url)
                    .await
                    .expect("Failed to connect to mysql");

                connection
                    .execute(format!("DROP DATABASE {};", db_name).as_str())
                    .await
                    .expect("Failed to drop database.");
            }
        }
    }
}
