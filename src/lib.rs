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
//! - TableManager: This trait is responsible for the communication between
//! sharing server and the source system for shares, schemas and tables.
//! - TableReader: This trait is responsible for reading tables of a specified
//! table format
//! - UrlSigner: This trait is responsible for signing the urls that will be
//! used to access the data.
//!
//! ```rust,no_run
//! # use std::sync::Arc;
//! use delta_sharing_server_rs::manager::dynamo::{DynamoConfig, DynamoTableManager};
//! use delta_sharing_server_rs::state::ShareApiState;
//! use delta_sharing_server_rs::router::get_router;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = aws_config::load_from_env().await;
//!     let client = aws_sdk_dynamodb::Client::new(&config);
//!       
//!     let table_manager_config = DynamoConfig::new("delta-sharing-store", "SK-PK-index");
//!     let table_manager = Arc::new(DynamoTableManager::new(client, table_manager_config));
//!     
//!     let state = ShareApiState::new(table_manager);
//!     let app = get_router(Arc::new(state));
//!     
//!     axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
//!         .serve(app.into_make_service())
//!         .await
//!         .unwrap();
//! }
//! ```

pub mod manager;
pub mod protocol;
pub mod reader;
pub mod signer;

mod error;
mod extract;
mod handler;
mod response;
pub mod router;
pub mod state;
