//! Types for implementing the Delta Sharing protocol.

mod action;
mod securable;
mod table;

pub use self::securable::{Schema, Share, Table};
pub use self::table::{Add, Cdf, DataFile, File, FileFormat, Metadata, Protocol, Remove};
