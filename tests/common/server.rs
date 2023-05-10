#![allow(dead_code)]

use axum::http::header::CONTENT_TYPE;
use axum::http::{
    header::{HeaderName, HeaderValue},
    StatusCode,
};
use axum::Server;
use bytes::Bytes;
use delta_sharing_server::manager::dynamo::DynamoShareReader;
use delta_sharing_server::reader::delta::DeltaTableReader;
use delta_sharing_server::router::build_sharing_server_router;
use delta_sharing_server::signer::s3::S3UrlSigner;
use delta_sharing_server::state::SharingServerState;
use std::net::{SocketAddr, TcpListener};
use std::{convert::TryFrom, sync::Arc};
use tower::make::Shared;

pub struct TestClient {
    client: reqwest::Client,
    addr: SocketAddr,
}

impl TestClient {
    pub async fn new() -> Self {
        let config = aws_config::load_from_env().await;
        let client = aws_sdk_dynamodb::Client::new(&config);

        // let dynamo_config = DynamoConfig::new("delta-sharing-store", "SK-PK-index");
        let table_manager = Arc::new(DynamoShareReader::new(
            client,
            "delta-sharing-store".to_owned(),
            "SK-PK-index".to_owned(),
        ));
        let mut state = SharingServerState::new(table_manager);

        state.add_table_reader("DELTA", Arc::new(DeltaTableReader));
        let s3_signer = S3UrlSigner::new(aws_sdk_s3::Client::new(&config));
        state.add_url_signer("s3", Arc::new(s3_signer));

        let app = build_sharing_server_router(Arc::new(state));

        let listener = TcpListener::bind("127.0.0.1:0").expect("Could not bind ephemeral socket");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let server = Server::from_tcp(listener).unwrap().serve(Shared::new(app));
            server.await.expect("server error");
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

    pub fn header<K, V>(mut self, key: K, value: V) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<axum::http::Error>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<axum::http::Error>,
    {
        self.builder = self.builder.header(key, value);
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
        assert_eq!(self.response.status(), StatusCode::OK);
    }

    pub fn assert_status_bad_request(&self) {
        assert_eq!(self.status(), StatusCode::BAD_REQUEST);
    }

    pub fn assert_status_not_found(&self) {
        assert_eq!(self.status(), StatusCode::NOT_FOUND);
    }

    pub fn assert_header_content_type_json(&self) {
        assert_eq!(
            self.response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json; charset=utf-8"
        );
    }

    pub fn assert_header_content_type_ndjson(&self) {
        assert_eq!(
            self.response.headers().get(CONTENT_TYPE).unwrap(),
            "application/x-ndjson; charset=utf-8"
        );
    }

    pub fn assert_header_table_version(&self, version: i32) {
        let rcv_version = self.headers().get("delta-table-version").unwrap();
        let exp_version = HeaderValue::from(version);
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

    pub fn status(&self) -> StatusCode {
        self.response.status()
    }

    pub fn headers(&self) -> &axum::http::HeaderMap {
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
