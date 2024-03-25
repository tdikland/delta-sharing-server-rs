#![allow(dead_code)]

use bytes::Bytes;
use delta_sharing_server::auth::{NoAuthLayer, RecipientId};
use delta_sharing_server::catalog::file::FileCatalog;
use delta_sharing_server::reader::delta::DeltaTableReader;
use delta_sharing_server::router::build_sharing_router;
use delta_sharing_server::signer::{noop::NoopSigner, registry::SignerRegistry};
use delta_sharing_server::state::SharingServerState;
use reqwest::header::{HeaderName, HeaderValue};
use std::io::Write;
use std::net::SocketAddr;
use std::{convert::TryFrom, sync::Arc};
use tempfile::NamedTempFile;
use testcontainers::clients::Cli;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use super::catalog::PostgresCatalogTestContext;

pub struct TestServer {
    addr: SocketAddr,
    client: reqwest::Client,
    docker: Cli,
    ctx: PostgresCatalogTestContext<'static>,
}

// impl TestServer {
//     pub async fn new() -> Self {
//         let docker = Cli::default();
//         let ctx = PostgresCatalogTestContext::new(&docker).await;

//         let client_id = ClientId::known("client");
//         let client = ctx.catalog().insert_client(&client_id).await.unwrap();

//         let share = ctx.catalog().insert_share("share").await.unwrap();
//         ctx.catalog()
//             .grant_access_to_share(&client.id, &share.id)
//             .await
//             .unwrap();

//         let schema = ctx
//             .catalog()
//             .insert_schema(&share.id, "schema")
//             .await
//             .unwrap();
//         ctx.catalog()
//             .grant_access_to_schema(&client.id, &schema.id)
//             .await
//             .unwrap();

//         let table = ctx
//             .catalog()
//             .insert_table(&schema.id, "table", "./tests/data/delta")
//             .await
//             .unwrap();
//         ctx.catalog()
//             .grant_access_to_table(&client.id, &table.id)
//             .await
//             .unwrap();

//         let catalog = Arc::new(ctx.catalog().clone());
//         let reader = Arc::new(DeltaTableReader);
//         let signer = Arc::new(NoopSigner);

//         let mut state = SharingServerState::new(catalog, reader);
//         state.add_url_signer("s3", signer);

//         let svc = build_sharing_server_router(Arc::new(state));
//         let app = svc.layer(TraceLayer::new_for_http()).layer(NoAuthLayer);

//         let listener = TcpListener::bind("127.0.0.1:0")
//             .await
//             .expect("Could not bind ephemeral socket");
//         let addr = listener.local_addr().unwrap();
//         tokio::spawn(async move {
//             axum::serve(listener, app).await.expect("server error");
//         });

//         let client = reqwest::Client::builder()
//             .redirect(reqwest::redirect::Policy::none())
//             .build()
//             .unwrap();

//         TestServer {
//             addr,
//             client,
//             docker,
//             ctx,
//         }
//     }
// }

impl TestServer {
    pub fn get(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.get(format!("http://{}{}", self.addr, url)),
        }
    }

    pub fn post(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.post(format!("http://{}{}", self.addr, url)),
        }
    }
}

pub struct TestClient {
    client: reqwest::Client,
    addr: SocketAddr,
}

impl TestClient {
    pub async fn new() -> Self {
        let tempfile = setup_share_config_file();
        let catalog = Arc::new(FileCatalog::new(tempfile.path().to_path_buf()));
        let reader = Arc::new(DeltaTableReader);

        let mut state = SharingServerState::new(catalog, reader, SignerRegistry::new());
        state.add_url_signer("s3", Arc::new(NoopSigner));

        let svc = build_sharing_router(Arc::new(state));
        let app = svc.layer(TraceLayer::new_for_http()).layer(NoAuthLayer);

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Could not bind ephemeral socket");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server error");
        });

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        TestClient { client, addr }
    }

    pub fn get(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.get(format!("http://{}{}", self.addr, url)),
        }
    }

    pub fn head(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.head(format!("http://{}{}", self.addr, url)),
        }
    }

    pub fn post(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.post(format!("http://{}{}", self.addr, url)),
        }
    }

    pub fn put(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.put(format!("http://{}{}", self.addr, url)),
        }
    }

    pub fn patch(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.patch(format!("http://{}{}", self.addr, url)),
        }
    }

    pub fn delete(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.delete(format!("http://{}{}", self.addr, url)),
        }
    }
}

pub struct RequestBuilder {
    builder: reqwest::RequestBuilder,
}

impl RequestBuilder {
    pub async fn send(self) -> TestResponse {
        TestResponse {
            response: self.builder.send().await.unwrap(),
        }
    }

    pub fn body(mut self, body: impl Into<reqwest::Body>) -> Self {
        self.builder = self.builder.body(body);
        self
    }

    pub fn json<T>(mut self, json: &T) -> Self
    where
        T: serde::Serialize,
    {
        self.builder = self.builder.json(json);
        self
    }

    pub fn header(mut self, key: impl AsRef<str>, value: &str) -> Self {
        self.builder = self
            .builder
            .header(key.as_ref().to_string(), value.to_string());
        self
    }
}

/// A wrapper around [`reqwest::Response`] that provides common methods with internal `unwrap()`s.
///
/// This is conventient for tests where panics are what you want. For access to
/// non-panicking versions or the complete `Response` API use `into_inner()` or
/// `as_ref()`.
#[derive(Debug)]
pub struct TestResponse {
    response: reqwest::Response,
}

impl TestResponse {
    pub fn assert_status_ok(&self) {
        assert_eq!(self.status(), reqwest::StatusCode::OK);
    }

    pub fn assert_status_bad_request(&self) {
        assert_eq!(self.status(), reqwest::StatusCode::BAD_REQUEST);
    }

    pub fn assert_status_not_found(&self) {
        assert_eq!(self.status(), reqwest::StatusCode::NOT_FOUND);
    }

    pub fn assert_header_content_type_json(&self) {
        assert_eq!(
            self.response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .unwrap(),
            "application/json; charset=utf-8"
        );
    }

    pub fn assert_header_content_type_ndjson(&self) {
        assert_eq!(
            self.response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .unwrap(),
            "application/x-ndjson; charset=utf-8"
        );
    }

    pub fn assert_header_table_version(&self, version: i32) {
        let rcv_version = self.headers().get("delta-table-version").unwrap();
        let exp_version = reqwest::header::HeaderValue::from(version);
        assert_eq!(rcv_version, exp_version);
    }

    pub async fn text(self) -> String {
        self.response.text().await.unwrap()
    }

    pub async fn bytes(self) -> Bytes {
        self.response.bytes().await.unwrap()
    }

    pub async fn json<T>(self) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        self.response.json().await.unwrap()
    }

    pub fn status(&self) -> reqwest::StatusCode {
        self.response.status()
    }

    pub fn headers(&self) -> &reqwest::header::HeaderMap {
        self.response.headers()
    }

    pub async fn chunk(&mut self) -> Option<Bytes> {
        self.response.chunk().await.unwrap()
    }

    pub async fn chunk_text(&mut self) -> Option<String> {
        let chunk = self.chunk().await?;
        Some(String::from_utf8(chunk.to_vec()).unwrap())
    }

    /// Get the inner [`reqwest::Response`] for less convenient but more complete access.
    pub fn into_inner(self) -> reqwest::Response {
        self.response
    }
}

impl AsRef<reqwest::Response> for TestResponse {
    fn as_ref(&self) -> &reqwest::Response {
        &self.response
    }
}

fn setup_share_config_file() -> NamedTempFile {
    let mut temp_file = NamedTempFile::new().unwrap();
    let shares_config = r#"
    version: 1
    shares:
    - name: "share"
      schemas:
      - name: "schema"
        tables:
        - name: "table"
          location: "./tests/data/delta"
          id: "00000000-0000-0000-0000-000000000000"
    "#;
    temp_file.write_all(shares_config.as_bytes()).unwrap();
    temp_file
}
