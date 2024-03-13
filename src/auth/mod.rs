//! Authentication middleware.

use std::{
    fmt::{self, Display},
    ops::Deref,
};

pub mod public;

/// Client identifier.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientId {
    /// Anonymous client identifier.
    Anonymous,
    /// Known client identifier.
    Known(String),
}

impl ClientId {
    /// Create a new unknown client identifier.
    pub fn anonymous() -> Self {
        Self::Anonymous
    }

    /// Create a new unknown client identifier.
    pub fn unknown() -> Self {
        Self::Anonymous
    }

    /// Create a new known client identifier.
    pub fn known<S: Into<String>>(client_id: S) -> Self {
        Self::Known(client_id.into())
    }

    /// Get the client identifier as a string.
    pub fn as_str(&self) -> &str {
        self
    }
}

impl Default for ClientId {
    fn default() -> Self {
        Self::Anonymous
    }
}

impl AsRef<str> for ClientId {
    fn as_ref(&self) -> &str {
        self
    }
}

impl Deref for ClientId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            ClientId::Anonymous => "ANONYMOUS",
            ClientId::Known(id) => id.as_str(),
        }
    }
}

impl Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientId::Anonymous => write!(f, "ANONYMOUS"),
            ClientId::Known(id) => write!(f, "{}", id.as_str()),
        }
    }
}
