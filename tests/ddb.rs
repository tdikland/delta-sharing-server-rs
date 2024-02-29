// use std::time::Duration;

// use aws_config::BehaviorVersion;
// use aws_sdk_dynamodb::{
//     types::{
//         AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ProvisionedThroughput,
//         ScalarAttributeType, TableStatus,
//     },
//     Client,
// };
// use delta_sharing_server::{
//     auth::ClientId,
//     catalog::{dynamo::DynamoCatalog, Pagination},
// };

// pub async fn setup_dynamo() {
//     // let _aws_region = env::var("AWS_REGION").expect("`AWS_REGION` is set");
//     // let _access_key = env::var("AWS_ACCESS_KEY_ID").expect("`AWS_ACCESS_KEY_ID` is set");
//     // let _access_secret = env::var("AWS_SECRET_ACCESS_KEY").expect("`AWS_ACCESS_KEY_ID` is set");

//     let config = aws_config::defaults(BehaviorVersion::latest())
//         .test_credentials()
//         .load()
//         .await;
//     let dynamodb_local_config = aws_sdk_dynamodb::config::Builder::from(&config)
//         // Override the endpoint in the config to use a local dynamodb server.
//         .endpoint_url(
//             // DynamoDB run locally uses port 8000 by default.
//             "http://localhost:8000",
//         )
//         .build();

//     let client = Client::from_conf(dynamodb_local_config);

//     let table_name = String::from("dynamo-test-table");

//     client
//         .create_table()
//         .table_name(table_name.clone())
//         .attribute_definitions(
//             AttributeDefinition::builder()
//                 .attribute_name("PK")
//                 .attribute_type(ScalarAttributeType::S)
//                 .build()
//                 .unwrap(),
//         )
//         .attribute_definitions(
//             AttributeDefinition::builder()
//                 .attribute_name("SK")
//                 .attribute_type(ScalarAttributeType::S)
//                 .build()
//                 .unwrap(),
//         )
//         .key_schema(
//             KeySchemaElement::builder()
//                 .attribute_name("PK")
//                 .key_type(KeyType::Hash)
//                 .build()
//                 .unwrap(),
//         )
//         .key_schema(
//             KeySchemaElement::builder()
//                 .attribute_name("SK")
//                 .key_type(KeyType::Range)
//                 .build()
//                 .unwrap(),
//         )
//         .billing_mode(BillingMode::Provisioned)
//         .provisioned_throughput(
//             ProvisionedThroughput::builder()
//                 .read_capacity_units(5)
//                 .write_capacity_units(5)
//                 .build()
//                 .unwrap(),
//         )
//         .send()
//         .await
//         .unwrap();

//     let resp = client.list_tables().send().await.unwrap();
//     println!("Tables: {:?}", resp);

//     let mut table_status = TableStatus::Creating;
//     while table_status != TableStatus::Active {
//         tokio::time::sleep(Duration::from_millis(500)).await;
//         let describe_table = client
//             .describe_table()
//             .table_name(table_name.clone())
//             .send()
//             .await
//             .unwrap();
//         table_status = describe_table
//             .table()
//             .unwrap()
//             .table_status()
//             .unwrap()
//             .clone();
//     }
// }

// use delta_sharing_server::catalog::Catalog;

// #[tokio::test]
// async fn test_setup_dynamo() {
//     // setup_dynamo().await;

//     let config = aws_config::defaults(BehaviorVersion::latest())
//         .test_credentials()
//         .endpoint_url("http://localhost:8000")
//         .load()
//         .await;
//     let client = Client::from_conf((&config).into());

//     let catalog = DynamoCatalog::new(client, "dynamo-test-table");

//     let shares = catalog
//         .list_shares(&ClientId::Anonymous, &Pagination::new(Some(2), None))
//         .await
//         .unwrap();

//     println!("Shares: {:?}", shares);
//     assert_eq!(shares.items().len(), 2);

//     let shares = catalog
//         .list_shares(
//             &ClientId::Anonymous,
//             &Pagination::new(
//                 Some(2),
//                 Some("eyJwayI6IkFOT05ZTU9VUyIsInNrIjoiU0hBUkUjc2hhcmUyIn0=".to_owned()),
//             ),
//         )
//         .await
//         .unwrap();

//     println!("Shares: {:?}", shares);
//     assert_eq!(shares.items().len(), 1);
//     assert_eq!(shares.next_page_token(), None);
// }
