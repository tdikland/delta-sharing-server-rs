use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::signer::UrlSigner;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Protocol {
    pub min_reader_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileFormat {
    pub provider: FileFormatProvider,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FileFormatProvider {
    Parquet,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Metadata {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub format: FileFormat,
    pub schema_string: String,
    pub partition_columns: Vec<String>,
    pub configuration: HashMap<String, Option<String>>,
    pub version: Option<String>,
    pub size: Option<u64>,
    pub num_files: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataFile {
    pub url: String,
    pub id: String,
    pub partition_values: HashMap<String, String>,
    pub size: u64,
    pub stats: Option<String>,
    pub version: Option<u64>,
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SignedDataFile {
    pub url: String,
    pub id: String,
    pub partition_values: HashMap<String, String>,
    pub size: u64,
    pub stats: Option<String>,
    pub version: Option<u64>,
    pub timestamp: Option<String>,
}

impl DataFile {
    pub async fn sign(self, signer: &dyn UrlSigner) -> SignedDataFile {
        let signed_url = signer.sign(&self.url).await;

        SignedDataFile {
            url: signed_url,
            id: self.id,
            partition_values: self.partition_values,
            size: self.size,
            stats: self.stats,
            version: self.version,
            timestamp: self.timestamp,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChangeFile {
    Add,
    Cdf,
    Remove,
}

impl ChangeFile {
    pub async fn sign(self, _signer: &dyn UrlSigner) -> SignedChangeFile {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SignedChangeFile {}
