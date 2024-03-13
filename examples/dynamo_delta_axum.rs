// use aws_config::BehaviorVersion;
// use delta_sharing_server::catalog::dynamo::{DynamoCatalog, DynamoCatalogConfig};
// use delta_sharing_server::reader::delta::DeltaTableReader;
// use delta_sharing_server::router::build_sharing_server_router;
// use delta_sharing_server::signer::s3::S3UrlSigner;
// use delta_sharing_server::state::SharingServerState;
// use std::sync::Arc;

#[tokio::main]
async fn main() {
    //     // setup tracing and logging
    //     tracing_subscriber::fmt()
    //         .with_max_level(tracing::Level::INFO)
    //         .init();

    //     // configure table manager
    //     let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    //     let ddb_client = aws_sdk_dynamodb::Client::new(&config);
    //     let catalog_config = DynamoCatalogConfig::new("test-table");
    //     let catalog = Arc::new(DynamoCatalog::new(ddb_client, catalog_config));

    //     // configure table readers
    //     let delta_table_reader = Arc::new(DeltaTableReader::new());

    //     // configure file url signers
    //     let s3_client = aws_sdk_s3::Client::new(&config);
    //     let s3_url_signer = Arc::new(S3UrlSigner::new(s3_client));

    //     // initialize server state
    //     let mut state = SharingServerState::new(catalog, delta_table_reader);
    //     state.add_url_signer("S3", s3_url_signer);

    //     // start server
    //     let app = build_sharing_server_router(Arc::new(state));
    //     let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    //     axum::serve(listener, app).await.unwrap();
}
