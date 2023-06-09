//! Trait for creating pre-signed urls.

use async_trait::async_trait;

mod adls;
mod gcs;
pub mod s3;

/// Trait implemented by object store clients to derive a pre-signed url from
/// a object store path/prefix.
#[mockall::automock]
#[async_trait]
pub trait UrlSigner: Send + Sync {
    /// Create a presigned url from a object store path.
    async fn sign(&self, path: &str) -> String;
}

// TODO: this is a better trait definition!!
// #[mockall::automock]
// #[async_trait]
// pub trait UrlSigner: Send + Sync {
//     /// Create a presigned url from a object store path.
//     async fn sign(&self, data_file: UnsignedTableData) -> SignedTableData;
// }