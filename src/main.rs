use lambda_http::{run, Error};

use delta_sharing_server_rs::manager::dynamo::DynamoTableManager;
use delta_sharing_server_rs::reader::delta::DeltaTableReader;
use delta_sharing_server_rs::router::build_sharing_server_router;
use delta_sharing_server_rs::signer::s3::S3UrlSigner;
use delta_sharing_server_rs::state::SharingServerState;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // required to enable CloudWatch error logging by the runtime
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // this needs to be set to false, otherwise ANSI color codes will
        // show up in a confusing manner in CloudWatch logs.
        .with_ansi(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    // configure table manager
    let config = aws_config::load_from_env().await;
    let ddb_client = aws_sdk_dynamodb::Client::new(&config);
    let table_manager = Arc::new(DynamoTableManager::new(
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
    let router = build_sharing_server_router(Arc::new(state));

    let app = tower::ServiceBuilder::new()
        .layer(axum_aws_lambda::LambdaLayer::default())
        .service(router);

    run(app).await
}
