use aws_config::BehaviorVersion;
use delta_sharing_server::auth::public::PublicAccessAuthLayer;
use delta_sharing_server::catalog::file::{FileCatalog, FileCatalogConfig};
use delta_sharing_server::reader::delta::DeltaTableReader;
use delta_sharing_server::router::build_sharing_router;
use delta_sharing_server::signer::registry::SignerRegistry;
use delta_sharing_server::signer::s3::S3UrlSigner;
use delta_sharing_server::state::SharingServerState;
use std::sync::Arc;
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() {
    // setup tracing and logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // configure catalog
    let config = FileCatalogConfig::new("./examples/config.yaml");
    let catalog = Arc::new(FileCatalog::new(config));

    // configure table reader
    let delta_table_reader = Arc::new(DeltaTableReader::new());

    // configure url signers
    let mut signers = SignerRegistry::new();

    let s3_conf = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&s3_conf);
    let s3_signer = S3UrlSigner::new(s3_client);
    signers.register("s3", Arc::new(s3_signer));

    // initialize server state
    let state = SharingServerState::new(catalog, delta_table_reader, signers);

    // start server
    let svc = build_sharing_router(Arc::new(state));
    let app = svc
        .layer(TraceLayer::new_for_http())
        .layer(PublicAccessAuthLayer::new());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
