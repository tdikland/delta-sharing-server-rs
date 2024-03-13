//! A registry of URL signers.

use std::{collections::HashMap, sync::Arc};

use super::{noop::NoopSigner, UrlSigner};

/// A registry of URL signers.
#[derive(Clone)]
pub struct SignerRegistry {
    registry: HashMap<String, Arc<dyn UrlSigner>>,
}

impl SignerRegistry {
    /// Create a new signer registry.
    pub fn new() -> Self {
        let registry = HashMap::new();
        Self { registry }
    }

    /// Register a new signer for a protocol.
    pub fn register(&mut self, protocol: &str, signer: Arc<dyn UrlSigner>) {
        self.registry.insert(protocol.to_string(), signer);
    }

    /// Get a signer for a protocol.
    pub fn get(&self, protocol: &str) -> Option<Arc<dyn UrlSigner>> {
        self.registry.get(protocol).cloned()
    }

    /// Get a signer for a protocol, or a NoopSigner if none is found.
    pub fn get_or_noop(&self, protocol: &str) -> Arc<dyn UrlSigner> {
        self.get(protocol).unwrap_or_else(|| Arc::new(NoopSigner))
    }
}

impl Default for SignerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
