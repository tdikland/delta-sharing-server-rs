// use delta_sharing_server_rs::manager::{
//     mysql::MySqlTableManager, postgres::PostgresTableManager, ListCursor, TableManager,
//     TableManagerError,
// };
// use sqlx::{Connection, Executor, MySqlPool, PgConnection, PgPool};
// use uuid::Uuid;

// enum Manager {
//     Postgres(PostgresTableManager),
//     MySql(MySqlTableManager),
// }

// struct TableManagerIntegration {
//     db_connection_string: String,
//     db_name: String,
//     manager: Manager,
// }

// impl TableManagerIntegration {
//     async fn new_postgres(connection_url: &str) -> Self {
//         let pool = PgPool::connect(connection_url)
//             .await
//             .expect("Failed to connect to Postgres");
//         let manager = PostgresTableManager::from_pool(pool);
//         Self {
//             db_connection_string: connection_url.to_string(),
//             db_name: String::from("test"),
//             manager: Manager::Postgres(manager),
//         }
//     }

//     #[must_use]
//     fn as_postgres(&self) -> Option<&PostgresTableManager> {
//         if let Manager::Postgres(v) = &self.manager {
//             Some(v)
//         } else {
//             None
//         }
//     }

//     #[must_use]
//     fn as_my_sql(&self) -> Option<&MySqlTableManager> {
//         if let Manager::MySql(v) = &self.manager {
//             Some(v)
//         } else {
//             None
//         }
//     }

//     async fn new_mysql(connection_url: &str) -> Self {
//         let pool = MySqlPool::connect(connection_url)
//             .await
//             .expect("Failed to connect to MySQL");
//         let manager = MySqlTableManager::from_pool(pool);
//         Self {
//             db_connection_string: connection_url.to_string(),
//             db_name: String::from("test"),
//             manager: Manager::MySql(manager),
//         }
//     }

//     // setup function that starts all resources before test
//     async fn setup(&mut self) {
//         self.start().await;
//         self.set_state().await
//     }

//     // teardown function that closes all resources after test
//     async fn teardown(&mut self) {
//         self.terminate().await
//     }

//     async fn start(&mut self) {
//         self.manager.start().await
//     }

//     async fn set_state(&mut self) {
//         self.manager.set_state().await
//     }

//     async fn terminate(&mut self) {
//         self.manager
//             .terminate(&self.db_connection_string, &self.db_name)
//             .await
//     }
// }

// impl Drop for TableManagerIntegration {
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

// impl Manager {
//     async fn start(&self) {
//         match self {
//             Manager::Postgres(pg_manager) => {
//                 sqlx::query(r#"CREATE DATABASE test;"#)
//                     .execute(pg_manager.pool())
//                     .await
//                     .expect("failed to create database");

//                 // sqlx::query(r#"USE test;"#)
//                 //     .execute(pg_manager.pool())
//                 //     .await
//                 //     .expect("failed to use database");

//                 sqlx::migrate!("src/manager/postgres/migrations")
//                     .run(pg_manager.pool())
//                     .await
//                     .unwrap();
//             }
//             Manager::MySql(mysql_manager) => {
//                 sqlx::query(r#"CREATE DATABASE test;"#)
//                     .execute(mysql_manager.pool())
//                     .await
//                     .unwrap();

//                 sqlx::migrate!("src/manager/mysql/migrations")
//                     .run(mysql_manager.pool())
//                     .await
//                     .unwrap();

//                 let share1 = mysql_manager.insert_share("share_1").await.unwrap();
//                 let share2 = mysql_manager.insert_share("share_2").await.unwrap();
//                 let _share3 = mysql_manager.insert_share("share_3").await.unwrap();

//                 let schema_11 = mysql_manager
//                     .insert_schema(&share1, "schema_1")
//                     .await
//                     .unwrap();
//                 let schema_12 = mysql_manager
//                     .insert_schema(&share1, "schema_2")
//                     .await
//                     .unwrap();
//                 let _schema_21 = mysql_manager
//                     .insert_schema(&share2, "schema_1")
//                     .await
//                     .unwrap();

//                 let _table_111 = mysql_manager
//                     .insert_table(
//                         &schema_11,
//                         "table_1",
//                         &format!("s3://bucket/table_111/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//                 let _table_112 = mysql_manager
//                     .insert_table(
//                         &schema_11,
//                         "table_2",
//                         &format!("s3://bucket/table_112/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//                 let _table_113 = mysql_manager
//                     .insert_table(
//                         &schema_11,
//                         "table_3",
//                         &format!("s3://bucket/table_113/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//                 let _table_114 = mysql_manager
//                     .insert_table(
//                         &schema_11,
//                         "table_4",
//                         &format!("s3://bucket/table_114/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//                 let _table_121 = mysql_manager
//                     .insert_table(
//                         &schema_12,
//                         "table_1",
//                         &format!("s3://bucket/table_121/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//                 let _table_122 = mysql_manager
//                     .insert_table(
//                         &schema_12,
//                         "table_2",
//                         &format!("s3://bucket/table_122/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//             }
//         }
//     }

//     async fn set_state(&mut self) {
//         match self {
//             Manager::Postgres(pg) => {
//                 let share1 = pg.insert_share("share_1").await.unwrap();
//                 let share2 = pg.insert_share("share_2").await.unwrap();
//                 let _share3 = pg.insert_share("share_3").await.unwrap();

//                 let schema_11 = pg.insert_schema(&share1, "schema_1").await.unwrap();
//                 let schema_12 = pg.insert_schema(&share1, "schema_2").await.unwrap();
//                 let _schema_21 = pg.insert_schema(&share2, "schema_1").await.unwrap();

//                 let _table_111 = pg
//                     .insert_table(
//                         &schema_11,
//                         "table_1",
//                         &format!("s3://bucket/table_111/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//                 let _table_112 = pg
//                     .insert_table(
//                         &schema_11,
//                         "table_2",
//                         &format!("s3://bucket/table_112/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//                 let _table_113 = pg
//                     .insert_table(
//                         &schema_11,
//                         "table_3",
//                         &format!("s3://bucket/table_113/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//                 let _table_114 = pg
//                     .insert_table(
//                         &schema_11,
//                         "table_4",
//                         &format!("s3://bucket/table_114/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//                 let _table_121 = pg
//                     .insert_table(
//                         &schema_12,
//                         "table_1",
//                         &format!("s3://bucket/table_121/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//                 let _table_122 = pg
//                     .insert_table(
//                         &schema_12,
//                         "table_2",
//                         &format!("s3://bucket/table_122/"),
//                         None,
//                     )
//                     .await
//                     .unwrap();
//             }
//             Manager::MySql(_) => todo!(),
//         }
//     }

//     async fn terminate(&mut self, db_connection_url: &str, db_name: &str) {
//         match self {
//             Manager::Postgres(pg) => {
//                 pg.pool().close().await;
//                 let mut connection = PgConnection::connect(db_connection_url)
//                     .await
//                     .expect("Failed to connect to Postgres");

//                 // Force drop all active connections to database
//                 connection
//                     .execute(
//                         format!(
//                             r#"
//                                     SELECT pg_terminate_backend(pg_stat_activity.pid)
//                                     FROM pg_stat_activity
//                                     WHERE pg_stat_activity.datname = '{}'
//                                     AND pid <> pg_backend_pid()
//                                     "#,
//                             db_name
//                         )
//                         .as_str(),
//                     )
//                     .await
//                     .expect("Failed to terminate current connections to test db");

//                 connection
//                     .execute(format!(r#"DROP DATABASE "{}";"#, db_name).as_str())
//                     .await
//                     .expect("Failed to drop database.");
//             }
//             Manager::MySql(_) => todo!(),
//         }
//     }
// }

// async fn list_shares<M: TableManager>(manager: &M) {
//     // it should list up to 100 shares by default
//     let res1 = manager.list_shares(&ListCursor::default()).await.unwrap();
//     let mut share_names = res1.iter().map(|s| s.name()).collect::<Vec<_>>();
//     share_names.sort();
//     assert_eq!(share_names, vec!["share_1", "share_2", "share_3"]);
//     assert!(res1.next_page_token().is_none());

//     // it should respect the max results parameter and return a next page token
//     let res2 = manager
//         .list_shares(&ListCursor::new(Some(1), None))
//         .await
//         .unwrap();
//     assert_eq!(res2.len(), 1);
//     assert!(res2.next_page_token().is_some());

//     // it should continue the list from the next page token
//     let res3 = manager
//         .list_shares(&ListCursor::new(Some(2), res2.next_page_token().cloned()))
//         .await
//         .unwrap();
//     assert_eq!(res3.len(), 2);
//     assert!(res3.next_page_token().is_some());
//     assert!(!res3.items().contains(res2.items().first().unwrap()));

//     // it should return an empty list when there are no more shares
//     let res4 = manager
//         .list_shares(&ListCursor::new(Some(2), res3.next_page_token().cloned()))
//         .await
//         .unwrap();
//     assert!(res4.is_empty());
//     assert!(res4.next_page_token().is_none());
// }

// async fn get_share<M: TableManager>(manager: &M) {
//     // it should return the share if it exists
//     let existing_share = manager.get_share("share_1").await.unwrap();
//     assert_eq!(existing_share.name(), "share_1");

//     // it should return an error if the share does not exist
//     let non_existing_share = manager.get_share("absent").await.unwrap_err();
//     assert_eq!(
//         non_existing_share,
//         TableManagerError::ShareNotFound {
//             share_name: "absent".to_string()
//         }
//     );
// }

// async fn list_schemas<M: TableManager>(manager: &M) {
//     // it should list up to 100 schemas by default
//     let res1 = manager
//         .list_schemas("share_1", &ListCursor::default())
//         .await
//         .unwrap();
//     let mut schema_names = res1.iter().map(|s| s.name()).collect::<Vec<_>>();
//     schema_names.sort();
//     assert_eq!(schema_names, vec!["schema_1", "schema_2"]);
//     assert!(res1.next_page_token().is_none());

//     // it should respect the max results parameter and return a next page token
//     let res2 = manager
//         .list_schemas("share_1", &ListCursor::new(Some(1), None))
//         .await
//         .unwrap();
//     assert_eq!(res2.len(), 1);
//     assert!(res2.next_page_token().is_some());

//     // it should continue the list from the next page token
//     let res3 = manager
//         .list_schemas(
//             "share_1",
//             &ListCursor::new(Some(1), res2.next_page_token().cloned()),
//         )
//         .await
//         .unwrap();
//     assert_eq!(res3.len(), 1);
//     assert!(res3.next_page_token().is_some());
//     assert!(!res3.items().contains(res2.items().first().unwrap()));

//     // it should return an empty list when there are no more schemas
//     let res4 = manager
//         .list_schemas(
//             "share_1",
//             &ListCursor::new(Some(1), res3.next_page_token().cloned()),
//         )
//         .await
//         .unwrap();
//     assert!(res4.is_empty());
//     assert!(res4.next_page_token().is_none());
// }

// async fn list_tables_in_share<M: TableManager>(manager: &M) {
//     // it should list up to 100 tables by default
//     let res1 = manager
//         .list_tables_in_share("share_1", &ListCursor::default())
//         .await
//         .unwrap();
//     let mut table_names = res1.iter().map(|s| s.to_string()).collect::<Vec<_>>();
//     table_names.sort();
//     assert_eq!(
//         table_names,
//         vec![
//             "share_1.schema_1.table_1",
//             "share_1.schema_1.table_2",
//             "share_1.schema_1.table_3",
//             "share_1.schema_1.table_4",
//             "share_1.schema_2.table_1",
//             "share_1.schema_2.table_2"
//         ]
//     );
//     assert!(res1.next_page_token().is_none());

//     // it should respect the max results parameter and return a next page token
//     let res2 = manager
//         .list_tables_in_share("share_1", &ListCursor::new(Some(1), None))
//         .await
//         .unwrap();
//     assert_eq!(res2.len(), 1);
//     assert!(res2.next_page_token().is_some());

//     // it should continue the list from the next page token
//     let res3 = manager
//         .list_tables_in_share(
//             "share_1",
//             &ListCursor::new(Some(5), res2.next_page_token().cloned()),
//         )
//         .await
//         .unwrap();
//     assert_eq!(res3.len(), 5);
//     assert!(res3.next_page_token().is_some());
//     assert!(!res3.items().contains(res2.items().first().unwrap()));

//     // it should return an empty list when there are no more tables
//     let res4 = manager
//         .list_tables_in_share(
//             "share_1",
//             &ListCursor::new(Some(1), res3.next_page_token().cloned()),
//         )
//         .await
//         .unwrap();
//     assert!(res4.is_empty());
//     assert!(res4.next_page_token().is_none());
// }

// async fn list_tables_in_schema<M: TableManager>(manager: &M) {
//     // it should list up to 100 tables by default
//     let res1 = manager
//         .list_tables_in_schema("share_1", "schema_1", &ListCursor::default())
//         .await
//         .unwrap();
//     let mut table_names = res1.iter().map(|s| s.to_string()).collect::<Vec<_>>();
//     table_names.sort();
//     assert_eq!(
//         table_names,
//         vec![
//             "share_1.schema_1.table_1",
//             "share_1.schema_1.table_2",
//             "share_1.schema_1.table_3",
//             "share_1.schema_1.table_4"
//         ]
//     );
//     assert!(res1.next_page_token().is_none());

//     // it should respect the max results parameter and return a next page token
//     let res2 = manager
//         .list_tables_in_schema("share_1", "schema_1", &ListCursor::new(Some(1), None))
//         .await
//         .unwrap();
//     assert_eq!(res2.len(), 1);
//     assert!(res2.next_page_token().is_some());

//     // it should continue the list from the next page token
//     let res3 = manager
//         .list_tables_in_schema(
//             "share_1",
//             "schema_1",
//             &ListCursor::new(Some(3), res2.next_page_token().cloned()),
//         )
//         .await
//         .unwrap();
//     assert_eq!(res3.len(), 3);
//     assert!(res3.next_page_token().is_some());
//     assert!(!res3.items().contains(res2.items().first().unwrap()));

//     // it should return an empty list when there are no more tables
//     let res4 = manager
//         .list_tables_in_schema(
//             "share_1",
//             "schema_1",
//             &ListCursor::new(Some(1), res3.next_page_token().cloned()),
//         )
//         .await
//         .unwrap();
//     assert!(res4.is_empty());
//     assert!(res4.next_page_token().is_none());
// }

// async fn get_table<M: TableManager>(manager: &M) {
//     // it should return the table if it exists
//     let table = manager
//         .get_table("share_1", "schema_1", "table_1")
//         .await
//         .unwrap();
//     assert_eq!(table.name(), "table_1");
//     assert_eq!(table.share_name(), "share_1");
//     assert_eq!(table.schema_name(), "schema_1");
//     assert_eq!(table.storage_path(), "s3://bucket/table_111/");

//     // it should return an error if the share does not exist
//     assert!(matches!(
//         manager
//             .get_table("absent_share", "schema_1", "table_1")
//             .await,
//         Err(TableManagerError::ShareNotFound { .. })
//     ));

//     // it should return an error if the schema does not exist
//     assert!(matches!(
//         manager
//             .get_table("share_1", "absent_schema", "table_1")
//             .await,
//         Err(TableManagerError::SchemaNotFound { .. })
//     ));

//     // it should return an error if the table does not exist
//     assert!(matches!(
//         manager
//             .get_table("share_1", "schema_1", "absent_table")
//             .await,
//         Err(TableManagerError::TableNotFound { .. })
//     ));
// }

// #[tokio::test]
// async fn postgres() {
//     let connection_url = "postgres://postgres:postgrespw@localhost:32770";
//     let mut integ = TableManagerIntegration::new_postgres(connection_url).await;
//     integ.setup().await;
//     let manager = integ
//         .as_postgres()
//         .expect("could not get postgres integration context");

//     list_shares(manager).await;
//     get_share(manager).await;
//     list_schemas(manager).await;
//     list_tables_in_share(manager).await;
//     list_tables_in_schema(manager).await;
//     get_table(manager).await;
// }

// // #[tokio::test]
// // async fn mysql() {
// //     let integ =
// //         TableManagerIntegration::new_mysql("mysql://root:password@localhost:55000/mysql").await;
// //     integ.setup_shared_tables().await;
// //     let manager = integ.as_my_sql().unwrap();

// //     list_shares(manager).await;
// //     get_share(manager).await;
// //     list_schemas(manager).await;
// //     list_tables_in_share(manager).await;
// //     list_tables_in_schema(manager).await;
// //     get_table(manager).await;
// // }
