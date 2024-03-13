use http::header::AUTHORIZATION;

use insta::{assert_snapshot, with_settings};

mod common;
use common::api::TestServer;
use testcontainers::clients::Cli;
use uuid::Uuid;

#[tokio::test]
async fn list_shares_success() {
    let docker = Cli::default();
    let test_client = TestServer::new(&docker).await;

    let response = test_client
        .get("/shares")
        .header(AUTHORIZATION, "Bearer valid_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();

    let response_text = response.text().await;
    assert_response_snapshot!(response_text);
}

#[tokio::test]
async fn list_shares_pagination() {
    let docker = Cli::default();
    let test_client = TestServer::new(&docker).await;

    let response = test_client
        .get("/shares?maxResults=1")
        .header(AUTHORIZATION, "Bearer valid_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();

    let response_text = response.text().await;
    assert_response_snapshot!(response_text);

    let response = test_client
        .get(&format!(
            "/shares?maxResults=1&pageToken={}",
            Uuid::new_v4()
        ))
        .header(AUTHORIZATION, "Bearer valid_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();

    let response_text = response.text().await;
    assert_response_snapshot!(response_text);
}

#[tokio::test]
async fn list_shares_bad_page_token() {
    let docker = Cli::default();
    let test_client = TestServer::new(&docker).await;

    let response = test_client
        .get("/shares?pageToken=malformed_token")
        .header(AUTHORIZATION, "Bearer valid_token")
        .send()
        .await;

    response.assert_status_bad_request();
    response.assert_header_content_type_json();

    let response_text = response.text().await;
    assert_response_snapshot!(response_text);
}

#[tokio::test]
async fn get_share_success() {
    let docker = Cli::default();
    let test_client = TestServer::new(&docker).await;

    let response = test_client
        .get("/shares/share1")
        .header(AUTHORIZATION, "Bearer valid_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();

    let response_text = response.text().await;
    assert_response_snapshot!(response_text);
}

#[tokio::test]
async fn get_share_not_found() {
    let docker = Cli::default();
    let test_client = TestServer::new(&docker).await;

    let response = test_client
        .get("/shares/not-existing-share")
        .header(AUTHORIZATION, "Bearer valid_token")
        .send()
        .await;

    response.assert_status_not_found();
    response.assert_header_content_type_json();

    let response_text = response.text().await;
    assert_response_snapshot!(response_text);
}

#[tokio::test]
async fn list_schemas_success() {
    let docker = Cli::default();
    let test_client = TestServer::new(&docker).await;

    let response = test_client
        .get("/shares/share1/schemas")
        .header(AUTHORIZATION, "Bearer valid_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();

    let response_text = response.text().await;
    assert_response_snapshot!(response_text);
}

#[tokio::test]
async fn list_tables_in_schema_success() {
    let docker = Cli::default();
    let test_client = TestServer::new(&docker).await;

    let response = test_client
        .get("/shares/share1/schemas/schema1/tables")
        .header(AUTHORIZATION, "Bearer valid_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();

    let response_text = response.text().await;
    assert_response_snapshot!(response_text);
}

#[tokio::test]
async fn list_tables_in_share_success() {
    let docker = Cli::default();
    let test_client = TestServer::new(&docker).await;

    let response = test_client
        .get("/shares/share1/all-tables")
        .header(AUTHORIZATION, "Bearer valid_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_content_type_json();

    let response_text = response.text().await;
    assert_response_snapshot!(response_text);
}

#[tokio::test]
async fn get_table_version_latest_success() {
    let docker = Cli::default();
    let test_client = TestServer::new(&docker).await;

    let response = test_client
        .get("/shares/share1/schemas/schema1/tables/table1/version")
        .header(AUTHORIZATION, "Bearer valid_token")
        .send()
        .await;

    dbg!(&response);

    response.assert_status_ok();
    response.assert_header_table_version(1);

    let response_text = response.text().await;
    assert_response_snapshot!(response_text);
}

#[tokio::test]
async fn get_table_metadata_success() {
    let docker = Cli::default();
    let test_client = TestServer::new(&docker).await;

    let response = test_client
        .get("/shares/share1/schemas/schema1/tables/table1/metadata")
        .header(AUTHORIZATION, "Bearer valid_token")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_table_version(1);
    response.assert_header_content_type_ndjson();

    assert_snapshot!(response.text().await);
}

#[tokio::test]
async fn get_table_data_success() {
    tracing_subscriber::fmt::init();

    let docker = Cli::default();
    let test_client = TestServer::new(&docker).await;

    let response = test_client
        .post("/shares/share1/schemas/schema1/tables/table1/query")
        .header(AUTHORIZATION, "Bearer valid_token")
        .body("{}")
        .send()
        .await;

    response.assert_status_ok();
    response.assert_header_table_version(1);
    response.assert_header_content_type_ndjson();

    assert_snapshot!(response.text().await);
}
