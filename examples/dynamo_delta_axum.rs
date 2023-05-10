use delta_sharing_server::manager::dynamo::DynamoShareReader;
use delta_sharing_server::reader::delta::DeltaTableReader;
use delta_sharing_server::router::build_sharing_server_router;
use delta_sharing_server::signer::s3::S3UrlSigner;
use delta_sharing_server::state::SharingServerState;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // setup tracing and logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // configure table manager
    let config = aws_config::load_from_env().await;
    let ddb_client = aws_sdk_dynamodb::Client::new(&config);
    let table_manager = Arc::new(DynamoShareReader::new(
        ddb_client,
        "delta-sharing-store".to_owned(),
        "SK-PK-index".to_owned(),
    ));

    // configure table readers
    let delta_table_reader = Arc::new(DeltaTableReader::new());

    // configure file url signers
    let s3_client = aws_sdk_s3::Client::new(&config);
    let s3_url_signer = Arc::new(S3UrlSigner::new(s3_client));

    // initialize server state
    let mut state = SharingServerState::new(table_manager);
    state.add_table_reader("DELTA", delta_table_reader);
    state.add_url_signer("S3", s3_url_signer);

    // start server
    let app = build_sharing_server_router(Arc::new(state));
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
