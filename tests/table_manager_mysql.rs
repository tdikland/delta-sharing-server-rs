// use delta_sharing_server_rs::manager::{mysql::MySqlTableManager, ListCursor, TableManager};
// use sqlx::MySqlPool;
// use uuid::Uuid;

// async fn insert_share(pool: &MySqlPool, name: &str) -> String {
//     let uuid = Uuid::new_v4().to_string();
//     sqlx::query("INSERT INTO share (id, name) VALUES (?, ?)")
//         .bind(&uuid)
//         .bind(name)
//         .execute(pool)
//         .await
//         .unwrap();

//     uuid
// }

// async fn insert_schema(pool: &MySqlPool, name: &str, share_id: &str) -> String {
//     let uuid = Uuid::new_v4().to_string();
//     sqlx::query("INSERT INTO `schema` (id, name, share_id) VALUES (?, ?, ?)")
//         .bind(&uuid)
//         .bind(name)
//         .bind(share_id)
//         .execute(pool)
//         .await
//         .unwrap();

//     uuid
// }

// async fn insert_table(pool: &MySqlPool, name: &str, schema_id: &str) -> String {
//     let uuid = Uuid::new_v4().to_string();
//     sqlx::query("INSERT INTO `table` (id, name, schema_id, storage_path) VALUES (?, ?, ?, ?)")
//         .bind(&uuid)
//         .bind(name)
//         .bind(schema_id)
//         .bind(format!("s3://bucket/{}", name))
//         .execute(pool)
//         .await
//         .unwrap();

//     uuid
// }

// async fn delete_shares(pool: &MySqlPool) {
//     sqlx::query("DELETE FROM `table`")
//         .execute(pool)
//         .await
//         .unwrap();

//     sqlx::query("DELETE FROM `schema`")
//         .execute(pool)
//         .await
//         .unwrap();

//     sqlx::query("DELETE FROM share")
//         .execute(pool)
//         .await
//         .unwrap();
// }

// async fn setup_tables(pool: &MySqlPool) {
//     sqlx::migrate!("src/manager/mysql/migrations")
//         .run(pool)
//         .await
//         .unwrap();

//     // shared-securable-structure
//     // |- share_1
//     // |  |- schema_1
//     // |  |  |- table_1 (s3://bucket11/table_1)
//     // |  |  |- table_2 (s3://bucket11/table_2)
//     // |  |  |- table_3 (s3://bucket11/table_3)
//     // |  |  |- table_4 (s3://bucket11/table_4)
//     // |  |- schema_2
//     // |  |  |- table_1 (s3://bucket12/table_1)
//     // |  |  |- table_2 (s3://bucket12/table_2)
//     // |- share_2
//     // |  |- schema_1
//     // |  |  |- table_1 (s3://bucket21/table_1)
//     // |  |  |- table_2 (s3://bucket21/table_2)
//     // |- share_3

//     delete_shares(pool).await;
//     let share_id1 = insert_share(pool, "share_1").await;
//     let share_id2 = insert_share(pool, "share_2").await;
//     let _share_id3 = insert_share(pool, "share_3").await;

//     let schema_id11 = insert_schema(pool, "schema_1", &share_id1).await;
//     let schema_id12 = insert_schema(pool, "schema_2", &share_id1).await;
//     let _schema_id21 = insert_schema(pool, "schema_1", &share_id2).await;

//     let _table_id111 = insert_table(pool, "table_1", &schema_id11).await;
//     let _table_id112 = insert_table(pool, "table_2", &schema_id11).await;
//     let _table_id113 = insert_table(pool, "table_3", &schema_id11).await;
//     let _table_id114 = insert_table(pool, "table_4", &schema_id11).await;
//     let _table_id121 = insert_table(pool, "table_1", &schema_id12).await;
//     let _table_id122 = insert_table(pool, "table_2", &schema_id12).await;
// }

// #[tokio::test]
// async fn list_tables() {
//     let table_manager = MySqlTableManager::new("mysql://root:password@localhost:55000/mysql").await;
//     setup_tables(table_manager.pool()).await;

//     let list_shares = table_manager
//         .list_shares(&ListCursor::default())
//         .await
//         .unwrap();
//     let mut share_names = list_shares
//         .into_iter()
//         .map(|s| s.name())
//         .collect::<Vec<_>>();
//     share_names.sort();
//     assert_eq!(share_names, vec!["share_1", "share_2", "share_3"]);

//     let share = table_manager.get_share("share_3").await.unwrap();
//     assert_eq!(share.name(), "share_3");

//     let schemas = table_manager
//         .list_schemas("share_1", &ListCursor::default())
//         .await
//         .unwrap();
//     assert_eq!(schemas.len(), 2);

//     let tables_in_share = table_manager
//         .list_tables_in_share("share_1", &ListCursor::default())
//         .await
//         .unwrap();
//     assert_eq!(tables_in_share.len(), 6);

//     let tables_in_schema = table_manager
//         .list_tables_in_schema("share_1", "schema_1", &ListCursor::default())
//         .await
//         .unwrap();
//     assert_eq!(tables_in_schema.len(), 4);

//     let table = table_manager
//         .get_table("share_1", "schema_1", "table_1")
//         .await
//         .unwrap();
//     assert_eq!(table.name(), "table_1");
//     assert_eq!(table.share_name(), "share_1");
//     assert_eq!(table.schema_name(), "schema_1");
//     assert_eq!(table.storage_path(), "s3://bucket/table_1");
// }
