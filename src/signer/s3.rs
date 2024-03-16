//! UrlSigner for S3 object paths.

use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::{presigning::PresigningConfig, Client};
use axum::http::Uri;

use super::{SignedUrl, UrlSigner};

/// Signing configuration for the S3 object store.
pub struct S3UrlSigner {
    client: Client,
}

impl S3UrlSigner {
    /// Create a new `S3UrlSigner` from the provided S3 SDK client.
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl UrlSigner for S3UrlSigner {
    async fn sign_url(&self, path: &str) -> SignedUrl {
        let uri = Uri::try_from(path).unwrap();
        let bucket = uri.host().unwrap();
        let key = &uri.path()[1..];

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(3600)).unwrap();
        // let expiration_time = presign_config.start_time() + presign_config.expires();
        let req = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .presigned(presign_config.clone())
            .await
            .unwrap();

        SignedUrl::new(
            req.uri().to_string(),
            presign_config.start_time().into(),
            presign_config.expires(),
        )
    }
}
