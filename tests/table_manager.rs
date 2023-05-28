use delta_sharing_server_rs::manager::{
    mysql::MySqlTableManager, postgres::PostgresTableManager, ListCursor, TableManager,
    TableManagerError,
};
use sqlx::{Connection, Executor, MySqlPool, PgConnection, PgPool};
use uuid::Uuid;

enum Manager {
    Postgres(PostgresTableManager),
    MySql(MySqlTableManager),
}

struct TableManagerIntegration {
    db_connection_string: String,
    db_name: String,
    manager: Manager,
}

impl TableManagerIntegration {
    async fn new_postgres(connection_url: &str) -> Self {
        let pool = PgPool::connect(connection_url)
            .await
            .expect("Failed to connect to Postgres");
        let manager = PostgresTableManager::from_pool(pool);
        Self {
            db_connection_string: connection_url.to_string(),
            db_name: String::from("test"),
            manager: Manager::Postgres(manager),
        }
    }

    async fn new_mysql(connection_url: &str) -> Self {
        let pool = MySqlPool::connect(connection_url)
            .await
            .expect("Failed to connect to MySQL");
        let manager = MySqlTableManager::from_pool(pool);
        Self {
            db_connection_string: connection_url.to_string(),
            db_name: String::from("test"),
            manager: Manager::MySql(manager),
        }
    }

    async fn initialize(&mut self) {
        match &self.manager {
            Manager::Postgres(pg) => {
                pg.pool()
                    .execute(format!(r#"CREATE DATABASE "{}";"#, self.db_name).as_str())
                    .await
                    .expect("Failed to create database.");

                pg.pool()
                    .execute(format!(r#"USE "{}";"#, self.db_name).as_str())
                    .await
                    .expect("Failed to use database.");

                pg.initialize().await;
            }
            Manager::MySql(_) => todo!(),
        }
    }

    async fn terminate(&mut self) {
        match &self.manager {
            Manager::Postgres(pg) => {
                pg.pool().close().await;
                let mut connection = PgConnection::connect(&self.db_connection_string)
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
                            self.db_name
                        )
                        .as_str(),
                    )
                    .await
                    .expect("Failed to terminate current connections to test db");

                connection
                    .execute(format!(r#"DROP DATABASE "{}";"#, self.db_name).as_str())
                    .await
                    .expect("Failed to drop database.");
            }
            Manager::MySql(_) => todo!(),
        }
    }

    #[must_use]
    fn as_postgres(&self) -> Option<&PostgresTableManager> {
        if let self.manager::Postgres(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[must_use]
    fn as_my_sql(&self) -> Option<&MySqlTableManager> {
        if let Self::MySql(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl Manager {
    async fn terminate(&mut self) {
        match self {
            Manager::Postgres(pg) => {
                pg.pool().close().await;
                let mut connection = PgConnection::connect(&self.db_connection_string)
                    .await
                    .expect("Failed to connect to Postgres");

                // Force drop all active connections to database
                // TODO: see if there is a softer way to handle this (i.e. close connection when DB access is complete)
                connection
                    .execute(
                        format!(
                            r#"
                                SELECT pg_terminate_backend(pg_stat_activity.pid)
                                FROM pg_stat_activity
                                WHERE pg_stat_activity.datname = '{}'
                                AND pid <> pg_backend_pid()
                                "#,
                            self.db_name
                        )
                        .as_str(),
                    )
                    .await
                    .expect("Failed to terminate current connections to test db");

                connection
                    .execute(format!(r#"DROP DATABASE "{}";"#, self.db_name).as_str())
                    .await
                    .expect("Failed to drop database.");
                println!("Database cleaned up successfully.")
            }
            TableManagerIntegration::MySql(_) => todo!(),
        }
    }

    async fn initialize(&self) {
        match self {
            Manager::Postgres(pg_manager) => {
                sqlx::query(r#"CREATE SCHEMA IF NOT EXISTS test;"#)
                    .execute(pg_manager.pool())
                    .await
                    .unwrap();

                sqlx::migrate!("src/manager/postgres/migrations")
                    .run(pg_manager.pool())
                    .await
                    .unwrap();

                pg_manager.delete_tables().await.unwrap();
                pg_manager.delete_schemas().await.unwrap();
                pg_manager.delete_shares().await.unwrap();

                let share1 = pg_manager.insert_share("share_1").await.unwrap();
                let share2 = pg_manager.insert_share("share_2").await.unwrap();
                let _share3 = pg_manager.insert_share("share_3").await.unwrap();

                let schema_11 = pg_manager.insert_schema(&share1, "schema_1").await.unwrap();
                let schema_12 = pg_manager.insert_schema(&share1, "schema_2").await.unwrap();
                let _schema_21 = pg_manager.insert_schema(&share2, "schema_1").await.unwrap();

                let _table_111 = pg_manager
                    .insert_table(
                        &schema_11,
                        "table_1",
                        &format!("s3://bucket/table_111/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_112 = pg_manager
                    .insert_table(
                        &schema_11,
                        "table_2",
                        &format!("s3://bucket/table_112/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_113 = pg_manager
                    .insert_table(
                        &schema_11,
                        "table_3",
                        &format!("s3://bucket/table_113/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_114 = pg_manager
                    .insert_table(
                        &schema_11,
                        "table_4",
                        &format!("s3://bucket/table_114/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_121 = pg_manager
                    .insert_table(
                        &schema_12,
                        "table_1",
                        &format!("s3://bucket/table_121/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_122 = pg_manager
                    .insert_table(
                        &schema_12,
                        "table_2",
                        &format!("s3://bucket/table_122/"),
                        None,
                    )
                    .await
                    .unwrap();
            }
            Manager::MySql(mysql_manager) => {
                sqlx::query(r#"CREATE DATABASE IF NOT EXISTS test;"#)
                    .execute(mysql_manager.pool())
                    .await
                    .unwrap();

                sqlx::migrate!("src/manager/mysql/migrations")
                    .run(mysql_manager.pool())
                    .await
                    .unwrap();

                mysql_manager.delete_tables().await.unwrap();
                mysql_manager.delete_schemas().await.unwrap();
                mysql_manager.delete_shares().await.unwrap();

                let share1 = mysql_manager.insert_share("share_1").await.unwrap();
                let share2 = mysql_manager.insert_share("share_2").await.unwrap();
                let _share3 = mysql_manager.insert_share("share_3").await.unwrap();

                let schema_11 = mysql_manager
                    .insert_schema(&share1, "schema_1")
                    .await
                    .unwrap();
                let schema_12 = mysql_manager
                    .insert_schema(&share1, "schema_2")
                    .await
                    .unwrap();
                let _schema_21 = mysql_manager
                    .insert_schema(&share2, "schema_1")
                    .await
                    .unwrap();

                let _table_111 = mysql_manager
                    .insert_table(
                        &schema_11,
                        "table_1",
                        &format!("s3://bucket/table_111/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_112 = mysql_manager
                    .insert_table(
                        &schema_11,
                        "table_2",
                        &format!("s3://bucket/table_112/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_113 = mysql_manager
                    .insert_table(
                        &schema_11,
                        "table_3",
                        &format!("s3://bucket/table_113/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_114 = mysql_manager
                    .insert_table(
                        &schema_11,
                        "table_4",
                        &format!("s3://bucket/table_114/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_121 = mysql_manager
                    .insert_table(
                        &schema_12,
                        "table_1",
                        &format!("s3://bucket/table_121/"),
                        None,
                    )
                    .await
                    .unwrap();
                let _table_122 = mysql_manager
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


}

impl Drop for TableManagerIntegration {
    fn drop(&mut self) {
        match self {
            TableManagerIntegration::Postgres(pg) => {
                std::thread::scope(|s| {
                    s.spawn(|| {
                        let runtime = tokio::runtime::Builder::new_multi_thread()
                            .enable_all()
                            .build()
                            .unwrap();
                        runtime.block_on(pg.terminate());
                    });
                });
            }
            TableManagerIntegration::MySql(_) => todo!(),
        }
    }
}

// impl Drop for TableManagerIntegration {
//     fn drop(&mut self) {
//         match self {
//             TableManagerIntegration::Postgres(pg) => {
//                 tokio::task::spawn(async move {
//                     sqlx::query("DROP DATABASE IF EXISTS test")
//                         .execute(pg.pool())
//                         .await
//                         .unwrap();
//                 });
//             }
//             TableManagerIntegration::MySql(mysql) => {
//                 let mut rt = tokio::runtime::Runtime::new().unwrap();
//                 rt.block_on(async move {
//                     sqlx::query("DROP DATABASE IF EXISTS test")
//                         .execute(mysql.pool())
//                         .await
//                         .unwrap();
//                 });
//             }
//         }
//     }
// }

async fn list_shares<M: TableManager>(manager: &M) {
    // it should list up to 100 shares by default
    let res1 = manager.list_shares(&ListCursor::default()).await.unwrap();
    let mut share_names = res1.iter().map(|s| s.name()).collect::<Vec<_>>();
    share_names.sort();
    assert_eq!(share_names, vec!["share_1", "share_2", "share_3"]);
    assert!(res1.next_page_token().is_none());

    // it should respect the max results parameter and return a next page token
    let res2 = manager
        .list_shares(&ListCursor::new(Some(1), None))
        .await
        .unwrap();
    assert_eq!(res2.len(), 1);
    assert!(res2.next_page_token().is_some());

    // it should continue the list from the next page token
    let res3 = manager
        .list_shares(&ListCursor::new(Some(2), res2.next_page_token().cloned()))
        .await
        .unwrap();
    assert_eq!(res3.len(), 2);
    assert!(res3.next_page_token().is_some());
    assert!(!res3.items().contains(res2.items().first().unwrap()));

    // it should return an empty list when there are no more shares
    let res4 = manager
        .list_shares(&ListCursor::new(Some(2), res3.next_page_token().cloned()))
        .await
        .unwrap();
    assert!(res4.is_empty());
    assert!(res4.next_page_token().is_none());
}

async fn get_share<M: TableManager>(manager: &M) {
    // it should return the share if it exists
    let existing_share = manager.get_share("share_1").await.unwrap();
    assert_eq!(existing_share.name(), "share_1");

    // it should return an error if the share does not exist
    let non_existing_share = manager.get_share("absent").await.unwrap_err();
    assert_eq!(
        non_existing_share,
        TableManagerError::ShareNotFound {
            share_name: "absent".to_string()
        }
    );
}

async fn list_schemas<M: TableManager>(manager: &M) {
    // it should list up to 100 schemas by default
    let res1 = manager
        .list_schemas("share_1", &ListCursor::default())
        .await
        .unwrap();
    let mut schema_names = res1.iter().map(|s| s.name()).collect::<Vec<_>>();
    schema_names.sort();
    assert_eq!(schema_names, vec!["schema_1", "schema_2"]);
    assert!(res1.next_page_token().is_none());

    // it should respect the max results parameter and return a next page token
    let res2 = manager
        .list_schemas("share_1", &ListCursor::new(Some(1), None))
        .await
        .unwrap();
    assert_eq!(res2.len(), 1);
    assert!(res2.next_page_token().is_some());

    // it should continue the list from the next page token
    let res3 = manager
        .list_schemas(
            "share_1",
            &ListCursor::new(Some(1), res2.next_page_token().cloned()),
        )
        .await
        .unwrap();
    assert_eq!(res3.len(), 1);
    assert!(res3.next_page_token().is_some());
    assert!(!res3.items().contains(res2.items().first().unwrap()));

    // it should return an empty list when there are no more schemas
    let res4 = manager
        .list_schemas(
            "share_1",
            &ListCursor::new(Some(1), res3.next_page_token().cloned()),
        )
        .await
        .unwrap();
    assert!(res4.is_empty());
    assert!(res4.next_page_token().is_none());
}

async fn list_tables_in_share<M: TableManager>(manager: &M) {
    // it should list up to 100 tables by default
    let res1 = manager
        .list_tables_in_share("share_1", &ListCursor::default())
        .await
        .unwrap();
    let mut table_names = res1.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    table_names.sort();
    assert_eq!(
        table_names,
        vec![
            "share_1.schema_1.table_1",
            "share_1.schema_1.table_2",
            "share_1.schema_1.table_3",
            "share_1.schema_1.table_4",
            "share_1.schema_2.table_1",
            "share_1.schema_2.table_2"
        ]
    );
    assert!(res1.next_page_token().is_none());

    // it should respect the max results parameter and return a next page token
    let res2 = manager
        .list_tables_in_share("share_1", &ListCursor::new(Some(1), None))
        .await
        .unwrap();
    assert_eq!(res2.len(), 1);
    assert!(res2.next_page_token().is_some());

    // it should continue the list from the next page token
    let res3 = manager
        .list_tables_in_share(
            "share_1",
            &ListCursor::new(Some(5), res2.next_page_token().cloned()),
        )
        .await
        .unwrap();
    assert_eq!(res3.len(), 5);
    assert!(res3.next_page_token().is_some());
    assert!(!res3.items().contains(res2.items().first().unwrap()));

    // it should return an empty list when there are no more tables
    let res4 = manager
        .list_tables_in_share(
            "share_1",
            &ListCursor::new(Some(1), res3.next_page_token().cloned()),
        )
        .await
        .unwrap();
    assert!(res4.is_empty());
    assert!(res4.next_page_token().is_none());
}

async fn list_tables_in_schema<M: TableManager>(manager: &M) {
    // it should list up to 100 tables by default
    let res1 = manager
        .list_tables_in_schema("share_1", "schema_1", &ListCursor::default())
        .await
        .unwrap();
    let mut table_names = res1.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    table_names.sort();
    assert_eq!(
        table_names,
        vec![
            "share_1.schema_1.table_1",
            "share_1.schema_1.table_2",
            "share_1.schema_1.table_3",
            "share_1.schema_1.table_4"
        ]
    );
    assert!(res1.next_page_token().is_none());

    // it should respect the max results parameter and return a next page token
    let res2 = manager
        .list_tables_in_schema("share_1", "schema_1", &ListCursor::new(Some(1), None))
        .await
        .unwrap();
    assert_eq!(res2.len(), 1);
    assert!(res2.next_page_token().is_some());

    // it should continue the list from the next page token
    let res3 = manager
        .list_tables_in_schema(
            "share_1",
            "schema_1",
            &ListCursor::new(Some(3), res2.next_page_token().cloned()),
        )
        .await
        .unwrap();
    assert_eq!(res3.len(), 3);
    assert!(res3.next_page_token().is_some());
    assert!(!res3.items().contains(res2.items().first().unwrap()));

    // it should return an empty list when there are no more tables
    let res4 = manager
        .list_tables_in_schema(
            "share_1",
            "schema_1",
            &ListCursor::new(Some(1), res3.next_page_token().cloned()),
        )
        .await
        .unwrap();
    assert!(res4.is_empty());
    assert!(res4.next_page_token().is_none());
}

async fn get_table<M: TableManager>(manager: &M) {
    // it should return the table if it exists
    let table = manager
        .get_table("share_1", "schema_1", "table_1")
        .await
        .unwrap();
    assert_eq!(table.name(), "table_1");
    assert_eq!(table.share_name(), "share_1");
    assert_eq!(table.schema_name(), "schema_1");
    assert_eq!(table.storage_path(), "s3://bucket/table_111/");

    // it should return an error if the share does not exist
    assert!(matches!(
        manager
            .get_table("absent_share", "schema_1", "table_1")
            .await,
        Err(TableManagerError::ShareNotFound { .. })
    ));

    // it should return an error if the schema does not exist
    assert!(matches!(
        manager
            .get_table("share_1", "absent_schema", "table_1")
            .await,
        Err(TableManagerError::SchemaNotFound { .. })
    ));

    // it should return an error if the table does not exist
    assert!(matches!(
        manager
            .get_table("share_1", "schema_1", "absent_table")
            .await,
        Err(TableManagerError::TableNotFound { .. })
    ));
}

#[tokio::test]
async fn postgres() {
    let integ =
        TableManagerIntegration::new_postgres("postgres://postgres:postgrespw@localhost:32770")
            .await;
    integ.setup_shared_tables().await;
    let manager = integ.as_postgres().unwrap();

    list_shares(manager).await;
    get_share(manager).await;
    list_schemas(manager).await;
    list_tables_in_share(manager).await;
    list_tables_in_schema(manager).await;
    get_table(manager).await;
}

// #[tokio::test]
// async fn mysql() {
//     let integ =
//         TableManagerIntegration::new_mysql("mysql://root:password@localhost:55000/mysql").await;
//     integ.setup_shared_tables().await;
//     let manager = integ.as_my_sql().unwrap();

//     list_shares(manager).await;
//     get_share(manager).await;
//     list_schemas(manager).await;
//     list_tables_in_share(manager).await;
//     list_tables_in_schema(manager).await;
//     get_table(manager).await;
// }
