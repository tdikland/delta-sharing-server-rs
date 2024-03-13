//! A signer that does not sign URLs.

use async_trait::async_trait;

use super::UrlSigner;

/// A signer that does not sign URLs.
#[derive(Debug)]
pub struct NoopSigner;

#[async_trait]
impl UrlSigner for NoopSigner {
    async fn sign_url(&self, path: &str) -> String {
        path.to_string()
    }
}
