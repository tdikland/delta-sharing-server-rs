//! Traits and types for creating pre-signed urls.

use async_trait::async_trait;

use crate::protocol::table::{
    SignedDataFile, SignedTableData, UnsignedDataFile, UnsignedTableData,
};

mod adls;
mod gcs;
pub mod s3;

/// Trait implemented by object store clients to derive a pre-signed url from
/// a object store path/prefix.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait UrlSigner: Send + Sync {
    /// Create a presigned url from a object store path.
    async fn sign_url(&self, path: &str) -> String;

    /// Create a presigned url for a object store path within a data file.
    async fn sign_data_file(&self, data_file: UnsignedDataFile) -> SignedDataFile {
        match data_file {
            UnsignedDataFile::File(mut file) => {
                let signed_url = self.sign_url(file.url()).await;
                *file.url_mut() = signed_url;
                SignedDataFile::File(file)
            }
            UnsignedDataFile::Add(mut add) => {
                add.url = self.sign_url(&add.url).await;
                SignedDataFile::Add(add)
            }
            UnsignedDataFile::Cdf(mut cdf) => {
                cdf.url = self.sign_url(&cdf.url).await;
                SignedDataFile::Cdf(cdf)
            }
            UnsignedDataFile::Remove(mut remove) => {
                remove.url = self.sign_url(&remove.url).await;
                SignedDataFile::Remove(remove)
            }
        }
    }

    /// Create presigned urls for all data files in a table version.
    async fn sign_table_data(&self, table_data: UnsignedTableData) -> SignedTableData {
        let mut signed_data_files = vec![];
        for data_file in table_data.data {
            signed_data_files.push(self.sign_data_file(data_file).await);
        }
        SignedTableData {
            version: table_data.version,
            protocol: table_data.protocol,
            metadata: table_data.metadata,
            data: signed_data_files,
        }
    }
}
