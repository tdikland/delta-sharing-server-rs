//! # Delta Sharing Server
//!
//! Delta Sharing Server provides the building blocks to easily setup a
//! server compatible with the `Delta Sharing` protocol.
//!
//! ## Delta Sharing protocol
//!
//! Delta Sharing is an open protocol for secure real-time exchange of
//! large datasets, which enables organizations to share data in real time
//! regardless of which computing platforms they use. It is a simple REST
//! protocol that securely shares access to part of a cloud dataset and
//! leverages modern cloud storage systems, such as S3, ADLS, or GCS, to
//! reliably transfer data.
//!
//! ## Design
//!
//! In order to provide flexibility, this crate is build around three core
//! abstractions.
//!
//! - ShareReader: This trait is responsible for the communication between
//! sharing server and the source system for shares, schemas and tables.
//! - TableReader: This trait is responsible for reading tables of a specified
//! table format
//! - UrlSigner: This trait is responsible for signing the urls that will be
//! used to access the data.
//!
//! ```rust,no_run
//! # use std::sync::Arc;
//! use delta_sharing_server::manager::dynamo::DynamoShareReader;
//! use delta_sharing_server::reader::delta::DeltaTableReader;
//! use delta_sharing_server::signer::s3::S3UrlSigner;
//! use delta_sharing_server::router::build_sharing_server_router;
//! use delta_sharing_server::state::SharingServerState;
//!
//! #[tokio::main]
//! async fn main() {
//!     // configure table manager
//!     let config = aws_config::load_from_env().await;
//!     let ddb_client = aws_sdk_dynamodb::Client::new(&config);
//!     let table_manager = Arc::new(DynamoShareReader::new(ddb_client, "delta-sharing-table".to_owned(), "GSI1".to_owned()));
//!         
//!     // configure table readers
//!     let delta_table_reader = Arc::new(DeltaTableReader::new());
//!
//!     // configure file url signers
//!     let s3_client = aws_sdk_s3::Client::new(&config);
//!     let s3_url_signer = Arc::new(S3UrlSigner::new(s3_client));
//!
//!     // initialize server state
//!     let mut state = SharingServerState::new(table_manager);
//!     state.add_table_reader("DELTA", delta_table_reader);
//!     state.add_url_signer("S3", s3_url_signer);
//!
//!     // start server
//!     let app = build_sharing_server_router(Arc::new(state));    
//!     axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
//!         .serve(app.into_make_service())
//!         .await
//!         .unwrap();
//! }
//! ```

#![warn(missing_docs)]

pub mod manager;
pub mod protocol;
pub mod reader;
pub mod signer;

pub mod error;
mod extract;
mod handler;
mod response;
pub mod router;
pub mod state;
