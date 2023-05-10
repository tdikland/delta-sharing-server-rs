use async_trait::async_trait;

pub mod s3;

#[async_trait]
pub trait UrlSigner: Send + Sync {
    async fn sign(&self, path: &str) -> String;
}
