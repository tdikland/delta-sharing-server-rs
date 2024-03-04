use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use delta_sharing_server::signer::{s3::S3UrlSigner, UrlSigner};

enum IntegrationContext {
    S3(S3UrlSigner),
}

impl IntegrationContext {
    pub fn new_s3(client: Client) -> Self {
        Self::S3(S3UrlSigner::new(client))
    }

    pub async fn sign(&self, path: &str) -> String {
        match self {
            Self::S3(signer) => signer.sign_url(path).await,
        }
    }
}

#[tokio::test]
async fn s3() {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = Client::new(&config);
    let ctx = IntegrationContext::new_s3(s3_client);

    let path = "s3://delta-sharing-server-rs-tests/file.txt";
    let signed_url = ctx.sign(path).await;

    let client = reqwest::Client::new();
    let res = client.get(signed_url).send().await.unwrap();
    assert_eq!(res.status(), 200);
}
