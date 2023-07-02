use delta_sharing_server::{
    manager::{ShareIoError, ShareReader},
    protocol::share::ListCursor,
};

mod common;
use common::table_manager::IntegrationContext;

#[tokio::test]
async fn postgres() {
    let mut ctx = IntegrationContext::setup_postgres().await;
    let manager = ctx.as_pg();

    test_list_shares(manager).await;
    test_get_share(manager).await;
    test_list_schemas(manager).await;
    test_list_tables_in_share(manager).await;
    test_list_tables_in_schema(manager).await;
    test_get_table(manager).await;

    ctx.teardown().await;
}

#[tokio::test]
async fn mysql() {
    let mut ctx = IntegrationContext::setup_mysql().await;
    let manager = ctx.as_mysql();

    test_list_shares(manager).await;
    test_get_share(manager).await;
    test_list_schemas(manager).await;
    test_list_tables_in_share(manager).await;
    test_list_tables_in_schema(manager).await;
    test_get_table(manager).await;

    ctx.teardown().await;
}

#[tokio::test]
async fn dynamodb() {
    let mut ctx = IntegrationContext::setup_dynamo().await;
    let manager = ctx.as_dynamo();

    test_list_shares(manager).await;
    test_get_share(manager).await;
    test_list_schemas(manager).await;
    test_list_tables_in_share(manager).await;
    test_list_tables_in_schema(manager).await;
    // test_get_table(manager).await;

    ctx.teardown().await;
}

async fn test_list_shares<M: ShareReader>(manager: &M) {
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
    assert!(!res3.items().contains(res2.items().first().unwrap()));

    // it should return an empty list when there are no more shares
    if let Some(final_page_token) = res3.next_page_token() {
        let res4 = manager
            .list_shares(&ListCursor::new(Some(2), Some(final_page_token.clone())))
            .await
            .unwrap();
        assert!(res4.is_empty());
        assert!(res4.next_page_token().is_none());
    }
}

async fn test_get_share<M: ShareReader>(manager: &M) {
    // it should return the share if it exists
    let existing_share = manager.get_share("share_1").await.unwrap();
    assert_eq!(existing_share.name(), "share_1");

    // it should return an error if the share does not exist
    let non_existing_share = manager.get_share("absent").await.unwrap_err();
    assert_eq!(
        non_existing_share,
        ShareIoError::ShareNotFound {
            share_name: "absent".to_string()
        }
    );
}

async fn test_list_schemas<M: ShareReader>(manager: &M) {
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
    assert!(!res3.items().contains(res2.items().first().unwrap()));

    // it should return an empty list when there are no more schemas
    if let Some(final_page_token) = res3.next_page_token() {
        let res4 = manager
            .list_schemas(
                "share_1",
                &ListCursor::new(Some(1), Some(final_page_token.clone())),
            )
            .await
            .unwrap();
        assert!(res4.is_empty());
        assert!(res4.next_page_token().is_none());
    }
}

async fn test_list_tables_in_share<M: ShareReader>(manager: &M) {
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
    assert!(!res3.items().contains(res2.items().first().unwrap()));

    // it should return an empty list when there are no more tables
    if let Some(final_page_token) = res3.next_page_token() {
        let res4 = manager
            .list_tables_in_share(
                "share_1",
                &ListCursor::new(Some(1), Some(final_page_token.clone())),
            )
            .await
            .unwrap();
        assert!(res4.is_empty());
        assert!(res4.next_page_token().is_none());
    }
}

async fn test_list_tables_in_schema<M: ShareReader>(manager: &M) {
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
    assert!(!res3.items().contains(res2.items().first().unwrap()));

    // it should return an empty list when there are no more tables
    if let Some(final_page_token) = res3.next_page_token() {
        let res4 = manager
            .list_tables_in_schema(
                "share_1",
                "schema_1",
                &ListCursor::new(Some(1), Some(final_page_token.clone())),
            )
            .await
            .unwrap();
        assert!(res4.is_empty());
        assert!(res4.next_page_token().is_none());
    }
}

async fn test_get_table<M: ShareReader>(manager: &M) {
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
    // assert!(matches!(
    //     manager
    //         .get_table("absent_share", "schema_1", "table_1")
    //         .await,
    //     Err(TableManagerError::ShareNotFound { .. })
    // ));

    // it should return an error if the schema does not exist
    // assert!(matches!(
    //     manager
    //         .get_table("share_1", "absent_schema", "table_1")
    //         .await,
    //     Err(TableManagerError::SchemaNotFound { .. })
    // ));

    // it should return an error if the table does not exist
    assert!(matches!(
        manager
            .get_table("share_1", "schema_1", "absent_table")
            .await,
        Err(ShareIoError::TableNotFound { .. })
    ));
}
