#![warn(missing_docs)]

//! Types for implementing the Delta Sharing protocol.

pub mod action;
pub mod securable;
pub mod share;
pub mod table;

pub use self::action::{Add, Cdf, File, FileFormat, Metadata, Protocol, Remove};
pub use self::securable::{Schema, Share, Table};
