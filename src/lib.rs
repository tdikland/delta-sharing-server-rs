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

#![warn(missing_docs)]

pub mod auth;
// pub mod protocol;
pub mod reader;
pub mod catalog;
pub mod signer;

pub mod error;
mod extract;
mod response;
pub mod router;
pub mod state;
