use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::signer::UrlSigner;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    pub min_reader_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileFormat {
    pub provider: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub format: FileFormat,
    pub schema_string: String,
    pub partition_columns: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub configuration: HashMap<String, Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_files: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DataFile {
    pub url: String,
    pub id: String,
    pub partition_values: HashMap<String, String>,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SignedDataFile {
    pub url: String,
    pub id: String,
    pub partition_values: HashMap<String, String>,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
