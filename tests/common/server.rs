#![allow(dead_code)]

use bytes::Bytes;
use delta_sharing_server::auth::NoAuthLayer;
use delta_sharing_server::catalog::file::FileCatalog;
use delta_sharing_server::reader::delta::DeltaTableReader;
use delta_sharing_server::router::build_sharing_server_router;
use delta_sharing_server::signer::NoopSigner;
use delta_sharing_server::state::SharingServerState;
use reqwest::header::{HeaderName, HeaderValue};
use std::io::Write;
use std::net::SocketAddr;
use std::{convert::TryFrom, sync::Arc};
use tempfile::NamedTempFile;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

pub struct TestClient {
    client: reqwest::Client,
    addr: SocketAddr,
}

impl TestClient {
    pub async fn new() -> Self {
        let tempfile = setup_share_config_file();
        let catalog = Arc::new(FileCatalog::new(tempfile.path().to_path_buf()));
        let reader = Arc::new(DeltaTableReader);

        let mut state = SharingServerState::new(catalog, reader);
        state.add_url_signer("s3", Arc::new(NoopSigner));

        let svc = build_sharing_server_router(Arc::new(state));
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
    let shares_config = r#"# The format version of this config file
    version: 1
    # Config shares/schemas/tables to share
    shares:
    - name: "share1"
      schemas:
      - name: "schema1"
        tables:
        - name: "table1"
          # S3. See https://github.com/delta-io/delta-sharing#s3 for how to config the credentials
          location: "s3a://<bucket-name>/<the-table-path>"
          id: "00000000-0000-0000-0000-000000000000"
        - name: "table2"
          # Azure Blob Storage. See https://github.com/delta-io/delta-sharing#azure-blob-storage for how to config the credentials
          location: "wasbs://<container-name>@<account-name}.blob.core.windows.net/<the-table-path>"
          id: "00000000-0000-0000-0000-000000000001"
    - name: "share2"
      schemas:
      - name: "schema2"
        tables:
        - name: "table3"
          # Azure Data Lake Storage Gen2. See https://github.com/delta-io/delta-sharing#azure-data-lake-storage-gen2 for how to config the credentials
          location: "abfss://<container-name>@<account-name}.dfs.core.windows.net/<the-table-path>"
          historyShared: true
          id: "00000000-0000-0000-0000-000000000002"
    - name: "share3"
      schemas:
      - name: "schema3"
        tables:
        - name: "table4"
          # Google Cloud Storage (GCS). See https://github.com/delta-io/delta-sharing#google-cloud-storage for how to config the credentials
          location: "gs://<bucket-name>/<the-table-path>"
          id: "00000000-0000-0000-0000-000000000003"
    - name: "share4"
      schemas:
        - name: "schema4"
          tables:
            - name: "table5"
              # Cloudflare R2. See https://github.com/delta-io/delta-sharing#cloudflare-r2 for how to config the credentials
              location: "s3a://<bucket-name>/<the-table-path>"
              id: "00000000-0000-0000-0000-000000000004"
    # Set the host name that the server will use
    host: "localhost"
    # Set the port that the server will listen on. Note: using ports below 1024 
    # may require a privileged user in some operating systems.
    port: 8080
    # Set the url prefix for the REST APIs
    endpoint: "/delta-sharing"
    # Set the timeout of S3 presigned url in seconds
    preSignedUrlTimeoutSeconds: 3600
    # How many tables to cache in the server
    deltaTableCacheSize: 10
    # Whether we can accept working with a stale version of the table. This is useful when sharing
    # static tables that will never be changed.
    stalenessAcceptable: false
    # Whether to evaluate user provided `predicateHints`
    evaluatePredicateHints: false
    # Whether to evaluate user provided `jsonPredicateHints`
    evaluateJsonPredicateHints: false
    # Whether to evaluate user provided `jsonPredicateHints` for V2 predicates.
    evaluateJsonPredicateHintsV2: false
    # The maximum page size permitted by queryTable/queryTableChanges API.
    queryTablePageSizeLimit: 10000
    # The TTL of the page token generated in queryTable/queryTableChanges API (in milliseconds).
    queryTablePageTokenTtlMs: 259200000
    # The TTL of the refresh token generated in queryTable API (in milliseconds).
    refreshTokenTtlMs: 3600000"#;
    temp_file.write_all(shares_config.as_bytes()).unwrap();
    temp_file
}
