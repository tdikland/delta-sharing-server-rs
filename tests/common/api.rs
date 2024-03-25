use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::Request,
    middleware::{self, Next},
    response::Response,
};
use delta_sharing_server::{
    auth::{NoAuthLayer, RecipientId},
    error::ServerError,
    reader::delta::DeltaTableReader,
    router::build_sharing_router,
    signer::{noop::NoopSigner, registry::SignerRegistry},
    state::SharingServerState,
};
use http::header::AUTHORIZATION;
use testcontainers::clients::Cli;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use super::catalog::PostgresCatalogTestContext;

async fn auth_middleware(mut request: Request, next: Next) -> Result<Response, ServerError> {
    if let Some(token) = request.headers().get(AUTHORIZATION) {
        let token = token.to_str().unwrap();
        if token == "Bearer valid_token" {
            let client_id = RecipientId::anonymous();
            tracing::info!(client_id=%client_id, "authenticated");
            request.extensions_mut().insert(client_id);
            let response = next.run(request).await;
            return Ok(response);
        }
    }

    Err(ServerError::unauthorized(""))
}

pub struct TestServer<'a> {
    addr: SocketAddr,
    client: reqwest::Client,
    ctx: PostgresCatalogTestContext<'a>,
}

impl<'a> TestServer<'a> {
    pub async fn new(docker: &'a Cli) -> Self {
        let ctx = PostgresCatalogTestContext::new(&docker).await;
        ctx.seed().await.unwrap();

        let catalog = Arc::new(ctx.catalog().clone());
        let reader = Arc::new(DeltaTableReader);
        let signer = Arc::new(NoopSigner);

        let mut state = SharingServerState::new(catalog, reader, SignerRegistry::new());
        state.add_url_signer("s3", signer);

        let svc = build_sharing_router(Arc::new(state));

        let app = svc
            .layer(TraceLayer::new_for_http())
            .layer(middleware::from_fn(auth_middleware));

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

        TestServer { addr, client, ctx }
    }

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

    pub fn asset_status_unauthorized(&self) {
        assert_eq!(self.status(), reqwest::StatusCode::UNAUTHORIZED);
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

    pub fn into_inner(self) -> reqwest::Response {
        self.response
    }
}

impl AsRef<reqwest::Response> for TestResponse {
    fn as_ref(&self) -> &reqwest::Response {
        &self.response
    }
}
