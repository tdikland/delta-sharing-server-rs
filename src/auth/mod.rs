//! Authentication middleware.

use std::{
    fmt::{self, Display},
    ops::Deref,
    task::{Context, Poll},
};

use axum::extract::Request;
use tower::{Layer, Service};

/// Client identifier.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientId {
    /// Anonymous client identifier.
    Anonymous,
    /// Known client identifier.
    Known(String),
}

impl ClientId {
    /// Create a new anonymous client identifier.
    pub fn anonymous() -> Self {
        Self::Anonymous
    }

    /// Create a new known client identifier.
    pub fn known(id: impl Into<String>) -> Self {
        Self::Known(id.into())
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

/// Authentication middleware.
///
/// Does not perform any authentication, but sets the client identifier to anonymous.
#[derive(Clone)]
pub struct NoAuthLayer;

impl<S> Layer<S> for NoAuthLayer {
    type Service = Auth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Auth { inner }
    }
}

/// Authentication middleware.
#[derive(Clone)]
pub struct Auth<S> {
    inner: S,
}

impl<S> Service<Request> for Auth<S>
where
    S: Service<Request> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request) -> Self::Future {
        let client_id = ClientId::Anonymous;
        req.extensions_mut().insert(client_id);
        self.inner.call(req)
    }
}
