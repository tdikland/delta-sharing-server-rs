

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Representation of the table protocol.
///
/// Protocol versioning will allow servers to exclude older clients that are
/// missing features required to correctly interpret their response if the
/// Delta Sharing Protocol evolves in the future. The protocol version will be
/// increased whenever non-forward-compatible changes are made to the
/// protocol. When a client is running an unsupported protocol version, it
/// should show an error message instructing the user to upgrade to a newer
/// version of their client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    /// The minimum version of the protocol that the client must support.
    pub min_reader_version: u32,
}

/// Representation of the table metadata.
///
/// The metadata of a table contains all the information required to correctly
/// interpret the data files of the table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    /// Unique table identifier
    pub id: String,
    /// Table name provided by the user
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Table description provided by the user
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Specification of the table format
    pub format: FileFormat,
    /// Schema of the table, serialized as a string. This string can be
    /// deserialized into a Schema type.
    pub schema_string: String,
    /// An array of column names that are used to partition the table.
    pub partition_columns: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    /// A map containing configuration options for the table.
    pub configuration: HashMap<String, Option<String>>,
    /// The version of the table this metadata corresponds to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The size of the table in bytes.
    pub size: Option<u64>,
    /// The number of files in the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_files: Option<u64>,
}

/// Representation of the table format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FileFormat {
    /// The format of the data files backing the shared table.
    pub provider: String,
}

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
// #[serde(rename_all = "camelCase")]
// pub enum DataFile {
//     File(File),
//     Add(Add),
//     Cdf(Cdf),
//     Remove(Remove),
// }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct File {
    pub url: String,
    pub id: String,
    pub partition_values: HashMap<String, String>,
    pub size: u64,
    pub stats: Option<String>,
    pub version: Option<u64>,
    pub timestamp: Option<String>,
    pub expiration_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    pub url: String,
    pub id: String,
    pub partition_values: HashMap<String, String>,
    pub size: u64,
    pub stats: Option<String>,
    pub version: Option<u64>,
    pub timestamp: Option<String>,
    pub expiration_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Cdf {
    pub url: String,
    pub id: String,
    pub partition_values: HashMap<String, String>,
    pub size: u64,
    pub stats: Option<String>,
    pub version: Option<u64>,
    pub timestamp: Option<String>,
    pub expiration_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    pub url: String,
    pub id: String,
    pub partition_values: HashMap<String, String>,
    pub size: u64,
    pub stats: Option<String>,
    pub version: Option<u64>,
    pub timestamp: Option<String>,
    pub expiration_timestamp: Option<String>,
}

// impl DataFile {
//     pub async fn sign(&mut self, signer: &dyn UrlSigner) {
//         match self {
//             DataFile::File(file) => file.url = signer.sign(&file.url).await,
//             DataFile::Add(add) => add.url = signer.sign(&add.url).await,
//             DataFile::Cdf(cdf) => cdf.url = signer.sign(&cdf.url).await,
//             DataFile::Remove(remove) => remove.url = signer.sign(&remove.url).await,
//         }
//     }
// }

// #![warn(missing_docs)]

// use std::collections::HashMap;

// use serde::{Deserialize, Serialize};

// use crate::signer::UrlSigner;

// /// Representation of the table protocol.
// ///
// /// Protocol versioning will allow servers to exclude older clients that are
// /// missing features required to correctly interpret their response if the
// /// Delta Sharing Protocol evolves in the future. The protocol version will be
// /// increased whenever non-forward-compatible changes are made to the
// /// protocol. When a client is running an unsupported protocol version, it
// /// should show an error message instructing the user to upgrade to a newer
// /// version of their client.
// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// #[serde(rename_all = "camelCase")]
// pub struct Protocol {
//     /// The minimum version of the protocol that the client must support.
//     pub min_reader_version: u32,
// }

// /// Representation of the table metadata.
// ///
// /// The metadata of a table contains all the information required to correctly
// /// interpret the data files of the table.
// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// #[serde(rename_all = "camelCase")]
// pub struct Metadata {
//     /// Unique table identifier
//     pub id: String,
//     /// Table name provided by the user
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub name: Option<String>,
//     /// Table description provided by the user
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub description: Option<String>,
//     /// Specification of the table format
//     pub format: FileFormat,
//     /// Schema of the table, serialized as a string. This string can be
//     /// deserialized into a Schema type.
//     pub schema_string: String,
//     /// An array of column names that are used to partition the table.
//     pub partition_columns: Vec<String>,
//     #[serde(skip_serializing_if = "HashMap::is_empty")]
//     /// A map containing configuration options for the table.
//     pub configuration: HashMap<String, Option<String>>,
//     /// The version of the table this metadata corresponds to.
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub version: Option<String>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     /// The size of the table in bytes.
//     pub size: Option<u64>,
//     /// The number of files in the table.
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub num_files: Option<u64>,
// }

// /// Representation of the table format.
// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// #[serde(rename_all = "camelCase")]
// pub struct FileFormat {
//     /// The format of the data files backing the shared table.
//     pub provider: String,
// }

// /// Representation of a table data file.
// ///
// /// A table consists of one or more data files.
// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// #[serde(rename_all = "camelCase")]
// pub struct DataFile {
//     pub url: String,
//     pub id: String,
//     pub partition_values: HashMap<String, String>,
//     pub size: u64,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub stats: Option<String>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub version: Option<u64>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub timestamp: Option<String>,
// }

// impl PartialOrd for DataFile {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         self.id.partial_cmp(&other.id)
//     }
// }

// /// Representation of a table data file.
// ///
// /// A table consists of one or more data files.
// // #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// // #[serde(rename_all = "camelCase")]
// // pub struct SignedDataFile {
// //     pub url: String,
// //     pub id: String,
// //     pub partition_values: HashMap<String, String>,
// //     pub size: u64,
// //     #[serde(skip_serializing_if = "Option::is_none")]
// //     pub stats: Option<String>,
// //     #[serde(skip_serializing_if = "Option::is_none")]
// //     pub version: Option<u64>,
// //     #[serde(skip_serializing_if = "Option::is_none")]
// //     pub timestamp: Option<String>,
// // }

// // impl DataFile {
// //     pub async fn sign(self, signer: &dyn UrlSigner) -> SignedDataFile {
// //         let signed_url = signer.sign(&self.url).await;

// //         SignedDataFile {
// //             url: signed_url,
// //             id: self.id,
// //             partition_values: self.partition_values,
// //             size: self.size,
// //             stats: self.stats,
// //             version: self.version,
// //             timestamp: self.timestamp,
// //         }
// //     }
// // }

// // #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// // #[serde(rename_all = "camelCase")]
// // pub enum ChangeFile {
// //     Add,
// //     Cdf,
// //     Remove,
// // }

// // impl ChangeFile {
// //     pub async fn sign(self, _signer: &dyn UrlSigner) -> SignedChangeFile {
// //         todo!()
// //     }
// // }

// // #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// // #[serde(rename_all = "camelCase")]
// // pub struct SignedChangeFile {}

// // pub struct DataFile {
// //     pub url: String,
// //     pub id: String,
// //     pub partition_values: HashMap<String, String>,
// //     pub size: u64,
// //     #[serde(skip_serializing_if = "Option::is_none")]
// //     pub stats: Option<String>,
// //     #[serde(skip_serializing_if = "Option::is_none")]
// //     pub version: Option<u64>,
// //     #[serde(skip_serializing_if = "Option::is_none")]
// //     pub timestamp: Option<String>,
// // }

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// #[serde(rename_all = "camelCase")]
// pub struct UnsignedDataFile {
//     pub kind: DataFileKind,
//     pub url: String,
//     pub id: String,
//     pub partition_values: HashMap<String, String>,
//     pub size: u64,
//     pub stats: Option<String>,
//     pub version: Option<u64>,
//     pub timestamp: Option<String>,
//     pub expiration_timestamp: Option<String>,
// }

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// #[serde(rename_all = "camelCase")]
// pub struct SignedDataFile {
//     pub kind: DataFileKind,
//     pub url: String,
//     pub id: String,
//     pub partition_values: HashMap<String, String>,
//     pub size: u64,
//     pub stats: Option<String>,
//     pub version: Option<u64>,
//     pub timestamp: Option<String>,
//     pub expiration_timestamp: Option<String>,
// }

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// #[serde(rename_all = "camelCase")]
// pub enum DataFileKind {
//     File,
//     Add,
//     Cdf,
//     Remove,
// }

// impl UnsignedDataFile {
//     pub async fn sign(self, signer: &dyn UrlSigner) -> SignedDataFile {
//         let signed_url = signer.sign(&self.url).await;

//         SignedDataFile {
//             kind: self.kind,
//             url: signed_url,
//             id: self.id,
//             partition_values: self.partition_values,
//             size: self.size,
//             stats: self.stats,
//             version: self.version,
//             timestamp: self.timestamp,
//             expiration_timestamp: self.expiration_timestamp,
//         }
//     }
// }
