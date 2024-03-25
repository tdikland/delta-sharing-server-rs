use std::{net::SocketAddr, sync::Arc};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Credentials, primitives::SdkBody};
use delta_sharing_server::{
    auth::public::PublicAccessAuthLayer, catalog::postgres::PostgresCatalog,
    reader::DeltaKernelReader, router::build_sharing_router,
    signer::registry::SignerRegistry, state::SharingServerState,
};
use sqlx::PgPool;
use testcontainers::{clients::Cli, Container, RunnableImage};
use testcontainers_modules::{localstack::LocalStack, postgres::Postgres};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

#[tokio::test]
async fn quey_table() {
    let docker = Cli::default();
    let server = TestServer::new(&docker).await;
}

struct TestServer<'a> {
    docker: &'a Cli,
    db: TestCatalog<'a>,
    storage: TestStorage<'a>,
    addr: SocketAddr,
}

impl<'a> TestServer<'a> {
    async fn new(docker: &'a Cli) -> Self {
        let db = TestCatalog::new(docker);
        let storage = TestStorage::new(docker);

        db.init().await;

        let catalog = Arc::new(PostgresCatalog::new(&db.url()).await);
        let reader = Arc::new(DeltaKernelReader::default());
        let state = SharingServerState::new(catalog, reader, SignerRegistry::new());

        let svc = build_sharing_router(Arc::new(state));
        let app = svc
            .layer(TraceLayer::new_for_http())
            .layer(PublicAccessAuthLayer::new());

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Could not bind ephemeral socket");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server error");
        });

        Self {
            docker,
            db,
            storage,
            addr,
        }
    }

    pub fn prepare_test_case(&self, case_name: &str) {
        let shared_conf = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url("http://127.0.0.1:4566")
            .load();
    }
}

struct TestCatalog<'a> {
    docker: &'a Cli,
    container: Container<'a, Postgres>,
}

impl<'a> TestCatalog<'a> {
    fn new(docker: &'a Cli) -> Self {
        let container = docker.run(Postgres::default());

        Self { docker, container }
    }

    fn url(&self) -> String {
        format!(
            "postgres://postgres:postgres@127.0.0.1:{}/postgres",
            self.container.get_host_port_ipv4(5432)
        )
    }

    async fn init(&self) {
        let pool = PgPool::connect(&self.url())
            .await
            .expect("Failed to connect to Postgres");

        sqlx::migrate!("tests/migrations/postgres")
            .run(&pool)
            .await
            .expect("Failed to run migrations");
    }
}

struct TestStorage<'a> {
    docker: &'a Cli,
    container: Container<'a, LocalStack>,
}

impl<'a> TestStorage<'a> {
    fn new(docker: &'a Cli) -> Self {
        let image: RunnableImage<_> = LocalStack::default().into();
        let image = image
            .with_env_var(("SERVICES", "s3"))
            .with_env_var(("DEBUG", "1"))
            .with_env_var(("S3_SKIP_SIGNATURE_VALIDATION", "0"))
            .with_env_var(("TEST_AWS_ACCESS_KEY_ID", "delta-sharing-key-id"))
            .with_env_var(("TEST_AWS_SECRET_ACCESS_KEY", "delta-sharing-secret-key"));
        let container = docker.run(image);

        Self { docker, container }
    }

    // async fn lala(&self) {
    //     let endpoint_url = format!(
    //         "http://127.0.0.1:{}",
    //         self.container.get_host_port_ipv4(4566)
    //     );
    //     let aws_conf = aws_config::defaults(BehaviorVersion::latest())
    //         .endpoint_url(endpoint_url)
    //         .credentials_provider(Credentials::for_tests())
    //         .load()
    //         .await;
    //     let s3 = aws_sdk_s3::Client::new(&aws_conf);

    //     s3.create_bucket()
    //         .bucket("delta-sharing-rs-integ-test")
    //         .send()
    //         .await
    //         .expect("Failed to create bucket");

        // s3.put_object().bucket("delta-sharing-rs-integ-test").key("table-with-dv-small").body(SdkBody::).send().await.expect("Failed to put object");
    }
}
