//! A signer that does not sign URLs.

use async_trait::async_trait;

use super::{SignedUrl, UrlSigner};

/// A signer that does not sign URLs.
#[derive(Debug)]
pub struct NoopSigner;

#[async_trait]
impl UrlSigner for NoopSigner {
    async fn sign_url(&self, path: &str) -> SignedUrl {
        SignedUrl::new(
            path.to_string(),
            chrono::Utc::now(),
            std::time::Duration::from_secs(3600),
        )
    }
}
