#![allow(dead_code)]

use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ProvisionedThroughput, ScalarAttributeType, TableStatus,
};
use delta_sharing_server::{
    manager::{dynamo::DynamoShareReader, mysql::MySqlShareReader, postgres::PostgresShareReader},
    protocol::securable::{Schema, SchemaBuilder, Share, ShareBuilder, Table, TableBuilder},
};
use sqlx::{Connection, Executor, MySqlConnection, PgConnection};
use std::{env, time::Duration};
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
        let pg_manager = PostgresShareReader::new(connection_url.as_str()).await;

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
        let mysql_manager = MySqlShareReader::new(connection_url.as_str()).await;

        let mut manager = Manager::MySql(mysql_manager);
        manager.setup().await;
        manager.prepare_data().await;

        Self {
            manager,
            db_url: db_url.to_string(),
            db_name: db_name.to_string(),
        }
    }

    pub async fn setup_dynamo() -> Self {
        let _aws_region = env::var("AWS_REGION").expect("`AWS_REGION` is set");
        let _access_key = env::var("AWS_ACCESS_KEY_ID").expect("`AWS_ACCESS_KEY_ID` is set");
        let _access_secret = env::var("AWS_SECRET_ACCESS_KEY").expect("`AWS_ACCESS_KEY_ID` is set");

        let config = aws_config::load_from_env().await;
        let client = aws_sdk_dynamodb::Client::new(&config);

        let table_name = format!("test-table-manager-{}", Uuid::new_v4());
        let index_name = "list-index";

        client
            .create_table()
            .table_name(table_name.clone())
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("PK")
                    .attribute_type(ScalarAttributeType::S)
                    .build(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("SK")
                    .attribute_type(ScalarAttributeType::S)
                    .build(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("PK")
                    .key_type(KeyType::Hash)
                    .build(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("SK")
                    .key_type(KeyType::Range)
                    .build(),
            )
            .billing_mode(BillingMode::Provisioned)
            .provisioned_throughput(
                ProvisionedThroughput::builder()
                    .read_capacity_units(5)
                    .write_capacity_units(5)
                    .build(),
            )
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name(index_name)
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("SK")
                            .key_type(KeyType::Hash)
                            .build(),
                    )
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("PK")
                            .key_type(KeyType::Range)
                            .build(),
                    )
                    .provisioned_throughput(
                        ProvisionedThroughput::builder()
                            .read_capacity_units(5)
                            .write_capacity_units(5)
                            .build(),
                    )
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::All)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let mut table_status = TableStatus::Creating;
        while table_status != TableStatus::Active {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let describe_table = client
                .describe_table()
                .table_name(table_name.clone())
                .send()
                .await
                .unwrap();
            table_status = describe_table
                .table()
                .unwrap()
                .table_status()
                .unwrap()
                .clone();
        }

        let ddb_manager = DynamoShareReader::new(client, table_name.clone(), index_name.to_owned());

        let mut manager = Manager::Dynamo(ddb_manager);
        manager.setup().await;
        manager.prepare_data().await;

        Self {
            manager,
            db_url: "".to_string(),
            db_name: table_name,
        }
    }

    pub fn from_dynamo(manager: DynamoShareReader, db_url: &str, db_name: &str) -> Self {
        Self {
            manager: Manager::Dynamo(manager),
            db_url: db_url.to_owned(),
            db_name: db_name.to_owned(),
        }
    }

    pub async fn teardown(&mut self) {
        self.manager.teardown(&self.db_url, &self.db_name).await
    }

    pub fn as_pg(&self) -> &PostgresShareReader {
        if let Manager::Postgres(pg) = &self.manager {
            pg
        } else {
            panic!("expected pg table manager")
        }
    }

    pub fn as_mysql(&self) -> &MySqlShareReader {
        if let Manager::MySql(mysql) = &self.manager {
            mysql
        } else {
            panic!("expected mysql table manager")
        }
    }

    pub fn as_dynamo(&self) -> &DynamoShareReader {
        if let Manager::Dynamo(ddb) = &self.manager {
            ddb
        } else {
            panic!("expected dynamo table manager")
        }
    }
}

// impl Drop for IntegrationContext {
//     fn drop(&mut self) {
//         std::thread::scope(|s| {
//             s.spawn(|| {
//                 let runtime = tokio::runtime::Builder::new_multi_thread()
//                     .enable_all()
//                     .build()
//                     .unwrap();
//                 runtime.block_on(self.teardown());
//             });
//         });
//     }
// }

#[derive(Debug)]
pub enum Manager {
    Postgres(PostgresShareReader),
    MySql(MySqlShareReader),
    Dynamo(DynamoShareReader),
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
            Manager::Dynamo(_) => (),
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
                    .insert_table(&schema_11, "table_1", "s3://bucket/table_111/", None)
                    .await
                    .unwrap();
                let _table_112 = pg
                    .insert_table(&schema_11, "table_2", "s3://bucket/table_112/", None)
                    .await
                    .unwrap();
                let _table_113 = pg
                    .insert_table(&schema_11, "table_3", "s3://bucket/table_113/", None)
                    .await
                    .unwrap();
                let _table_114 = pg
                    .insert_table(&schema_11, "table_4", "s3://bucket/table_114/", None)
                    .await
                    .unwrap();
                let _table_121 = pg
                    .insert_table(&schema_12, "table_1", "s3://bucket/table_121/", None)
                    .await
                    .unwrap();
                let _table_122 = pg
                    .insert_table(&schema_12, "table_2", "s3://bucket/table_122/", None)
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
                    .insert_table(&schema_11, "table_1", "s3://bucket/table_111/", None)
                    .await
                    .unwrap();
                let _table_112 = mysql
                    .insert_table(&schema_11, "table_2", "s3://bucket/table_112/", None)
                    .await
                    .unwrap();
                let _table_113 = mysql
                    .insert_table(&schema_11, "table_3", "s3://bucket/table_113/", None)
                    .await
                    .unwrap();
                let _table_114 = mysql
                    .insert_table(&schema_11, "table_4", "s3://bucket/table_114/", None)
                    .await
                    .unwrap();
                let _table_121 = mysql
                    .insert_table(&schema_12, "table_1", "s3://bucket/table_121/", None)
                    .await
                    .unwrap();
                let _table_122 = mysql
                    .insert_table(&schema_12, "table_2", "s3://bucket/table_122/", None)
                    .await
                    .unwrap();
            }
            Manager::Dynamo(ddb) => {
                fn build_share(share_number: &str) -> Share {
                    let name = format!("share_{}", share_number);
                    let id = format!("share_id_{}", share_number);
                    ShareBuilder::new(name).id(id).build()
                }

                fn build_schema(share_number: &str, schema_number: &str) -> Schema {
                    let share = build_share(share_number);
                    let schema_name = format!("schema_{}", schema_number);
                    let id = format!("schema_id_{}", schema_number);
                    SchemaBuilder::new(share, schema_name).id(id).build()
                }

                fn build_table(
                    share_number: &str,
                    schema_number: &str,
                    table_number: &str,
                ) -> Table {
                    let schema = build_schema(share_number, schema_number);
                    let table_name = format!("table_{}", table_number);
                    let storage_path = format!(
                        "s3://bucket/table_{}{}{}/",
                        share_number, schema_number, table_number
                    );
                    let table_id = format!("table_id_{}", table_number);
                    let table_format = "DELTA".to_owned();
                    TableBuilder::new(schema, table_name, storage_path)
                        .id(table_id)
                        .format(table_format)
                        .build()
                }

                let shares = ["1", "2", "3"]
                    .into_iter()
                    .map(build_share)
                    .collect::<Vec<Share>>();
                for share in shares {
                    ddb.put_share(share).await.unwrap();
                }

                // Add schemas to table manager
                let schemas = [("1", "1"), ("1", "2"), ("2", "1")]
                    .into_iter()
                    .map(|(share, schema)| build_schema(share, schema))
                    .collect::<Vec<Schema>>();
                for schema in schemas {
                    ddb.put_schema(schema).await.unwrap();
                }

                // Add tables to table manager
                let tables = [
                    ("1", "1", "1"),
                    ("1", "1", "2"),
                    ("1", "1", "3"),
                    ("1", "1", "4"),
                    ("1", "2", "1"),
                    ("1", "2", "2"),
                    ("2", "1", "1"),
                    ("2", "1", "2"),
                ]
                .into_iter()
                .map(|(sh, sch, t)| build_table(sh, sch, t))
                .collect::<Vec<Table>>();
                for table in tables {
                    ddb.put_table(table).await.unwrap();
                }

                // Wait for items to be eventually consistent
                tokio::time::sleep(Duration::from_secs(5)).await;
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
            Manager::Dynamo(ddb) => {
                dbg!(&db_name);
                ddb.client()
                    .delete_table()
                    .table_name(db_name)
                    .send()
                    .await
                    .expect("failed to delete ddb table");
            }
        }
    }
}
