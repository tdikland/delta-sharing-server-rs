use http::header::AUTHORIZATION;

use insta::assert_snapshot;

mod common;
use common::server::TestClient;

#[tokio::test]
async fn list_shares_success() {
    tracing_subscriber::fmt::try_init().ok();

    let test_client = TestClient::new().await;

    let response = test_client
        .get("/shares")
        .header(AUTHORIZATION, "Bearer foo_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();
    assert_snapshot!(response.text().await);
}

#[tokio::test]
async fn list_shares_pagination() {
    let test_client = TestClient::new().await;
    let response = test_client
        .get("/shares?maxResults=1")
        .header(AUTHORIZATION, "Bearer foo_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();
    assert_snapshot!(response.text().await);

    let token = "1";
    let response = test_client
        .get(&format!("/shares?maxResults=1&pageToken={}", token))
        .header(AUTHORIZATION, "Bearer foo_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();
    assert_snapshot!(response.text().await);
}

#[tokio::test]
async fn list_shares_bad_page_token() {
    let test_client = TestClient::new().await;
    let response = test_client
        .get("/shares?pageToken=malformed_token")
        .header(AUTHORIZATION, "Bearer foo_token")
        .send()
        .await;

    response.assert_status_bad_request();
    response.assert_header_content_type_json();
    assert_snapshot!(response.text().await);
}

#[tokio::test]
async fn get_share_success() {
    let test_client = TestClient::new().await;
    let response = test_client
        .get("/shares/share1")
        .header(AUTHORIZATION, "Bearer foo_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();
    assert_snapshot!(response.text().await);
}

#[tokio::test]
async fn get_share_not_found() {
    let test_client = TestClient::new().await;
    let response = test_client
        .get("/shares/not-existing-share")
        .header(AUTHORIZATION, "Bearer foo_token")
        .send()
        .await;

    response.assert_status_not_found();
    response.assert_header_content_type_json();
    assert_snapshot!(response.text().await);
}

#[tokio::test]
async fn list_schemas_success() {
    let test_client = TestClient::new().await;
    let response = test_client
        .get("/shares/share1/schemas")
        .header(AUTHORIZATION, "Bearer foo_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();
    assert_snapshot!(response.text().await);
}

// #[tokio::test]
// async fn list_tables_in_schema_success() {
//     let test_client = TestClient::new().await;
//     let response = test_client
//         .get("/shares/share1/schemas/schema1/tables")
//         .header(AUTHORIZATION, "Bearer foo_token")
//         .send()
//         .await;

//     response.assert_status_ok();
//     response.assert_header_content_type_json();
//     assert_snapshot!(response.text().await);
// }

// #[tokio::test]
// async fn list_tables_in_share_success() {
//     let test_client = TestClient::new().await;
//     let response = test_client
//         .get("/shares/share1/all-tables")
//         .header(AUTHORIZATION, "Bearer foo_token")
//         .send()
//         .await;

//     response.assert_status_ok();
//     response.assert_header_content_type_json();
//     assert_snapshot!(response.text().await);
// }

// #[tokio::test]
// async fn get_table_version_latest_success() {
//     let test_client = TestClient::new().await;
//     let response = test_client
//         .get("/shares/share1/schemas/schema1/tables/table1/version")
//         .header(AUTHORIZATION, "Bearer foo_token")
//         .send()
//         .await;

//     dbg!(&response);

//     response.assert_status_ok();
//     response.assert_header_table_version(2);
//     assert_snapshot!(response.text().await);
// }

// #[tokio::test]
// async fn get_table_metadata_success() {
//     let test_client = TestClient::new().await;
//     let response = test_client
//         .get("/shares/share1/schemas/schema1/tables/table1/metadata")
//         .header(AUTHORIZATION, "Bearer foo_token")
//         .send()
//         .await;

//     response.assert_status_ok();
//     response.assert_header_table_version(2);
//     response.assert_header_content_type_ndjson();
//     // assert_snapshot!(response.text().await);
// }
