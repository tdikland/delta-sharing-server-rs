//! Authentication middleware.

use std::{
    fmt::{self, Display},
    ops::Deref,
};

pub mod public;

/// Client identifier.
#[derive(Debug, Clone, PartialEq)]
pub enum RecipientId {
    /// Anonymous client identifier.
    Anonymous,
    /// Known client identifier.
    Known(String),
}

impl RecipientId {
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

impl Default for RecipientId {
    fn default() -> Self {
        Self::Anonymous
    }
}

impl AsRef<str> for RecipientId {
    fn as_ref(&self) -> &str {
        self
    }
}

impl Deref for RecipientId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            RecipientId::Anonymous => "ANONYMOUS",
            RecipientId::Known(id) => id.as_str(),
        }
    }
}

impl Display for RecipientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecipientId::Anonymous => write!(f, "ANONYMOUS"),
            RecipientId::Known(id) => write!(f, "{}", id.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recipient_id_display() {
        assert_eq!(
            RecipientId::anonymous().to_string(),
            String::from("ANONYMOUS")
        );
        assert_eq!(
            RecipientId::known("my_recipient_id").to_string(),
            String::from("my_recipient_id")
        );
    }
}
