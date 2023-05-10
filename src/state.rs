use std::{collections::HashMap, sync::Arc};

use crate::{
    manager::TableManager,
    reader::{delta::DeltaReader, TableReader},
    signer::UrlSigner,
};

#[derive(Clone)]
pub struct RouterState {
    shared_table_manager: Arc<dyn TableManager>,
    table_readers: HashMap<String, Arc<dyn TableReader>>,
    url_signers: HashMap<String, Arc<dyn UrlSigner>>,
}

impl RouterState {
    pub fn new(manager: Arc<dyn TableManager>) -> Self {
        let mut state = Self {
            shared_table_manager: manager,
            table_readers: HashMap::new(),
            url_signers: HashMap::new(),
        };

        // TODO: register default?
        let delta_reader = DeltaReader {};
        state
            .table_readers
            .insert("DELTA".to_owned(), Arc::new(delta_reader));

        state
    }

    pub fn add_table_reader(&mut self, format: impl Into<String>, reader: Arc<dyn TableReader>) {
        self.table_readers.insert(format.into(), reader);
    }

    pub fn add_url_signer(&mut self, storage: impl Into<String>, signer: Arc<dyn UrlSigner>) {
        self.url_signers.insert(storage.into(), signer);
    }

    pub fn table_manager(&self) -> Arc<dyn TableManager> {
        self.shared_table_manager.clone()
    }

    pub fn table_reader(&self, format: &str) -> Option<Arc<dyn TableReader>> {
        self.table_readers.get(format).cloned()
    }

    pub fn url_signer(&self, storage: &str) -> Option<Arc<dyn UrlSigner>> {
        self.url_signers.get(storage).cloned()
    }
}
