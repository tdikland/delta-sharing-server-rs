use std::{collections::HashMap, sync::Arc};

use super::{noop::NoopSigner, UrlSigner};

#[derive(Clone)]
pub struct SignerRegistry {
    registry: HashMap<String, Arc<dyn UrlSigner>>,
}

impl SignerRegistry {
    pub fn new() -> Self {
        let registry = HashMap::new();
        Self { registry }
    }

    pub fn register(&mut self, protocol: &str, signer: Arc<dyn UrlSigner>) {
        self.registry.insert(protocol.to_string(), signer);
    }

    pub fn get(&self, protocol: &str) -> Option<Arc<dyn UrlSigner>> {
        self.registry.get(protocol).cloned()
    }

    pub fn get_or_noop(&self, protocol: &str) -> Arc<dyn UrlSigner> {
        self.get(protocol).unwrap_or_else(|| Arc::new(NoopSigner))
    }
}

impl Default for SignerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
