//! Traits and types for creating pre-signed urls.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::response::TableActionsResponse;

pub mod registry;

mod adls;
mod gcs;
pub mod noop;
pub mod s3;

/// Trait implemented by object store clients to derive a pre-signed url from
/// a object store path/prefix.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait UrlSigner: Send + Sync {
    /// Create a presigned url from a object store path.
    async fn sign_url(&self, path: &str) -> SignedUrl;
}

/// A presigned url with a validity period.
pub struct SignedUrl {
    url: String,
    valid_from: DateTime<Utc>,
    valid_duration: Duration,
}

impl SignedUrl {
    pub fn new(url: String, valid_from: DateTime<Utc>, valid_duration: Duration) -> Self {
        Self {
            url,
            valid_from,
            valid_duration,
        }
    }

    /// Get the presigned url.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Get the time the presigned url expires.
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.valid_from + self.valid_duration
    }
}
