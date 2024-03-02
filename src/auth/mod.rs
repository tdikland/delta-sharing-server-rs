use std::{
    fmt::{self, Display},
    task::{Context, Poll},
};

use axum::extract::Request;
use tower::{Layer, Service};

#[derive(Debug, Clone, PartialEq)]
pub enum ClientId {
    Anonymous,
    Known(String),
}

impl ClientId {
    pub fn anonymous() -> Self {
        Self::Anonymous
    }

    pub fn known(id: impl Into<String>) -> Self {
        Self::Known(id.into())
    }
}

impl Default for ClientId {
    fn default() -> Self {
        Self::Anonymous
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

#[derive(Clone)]
pub struct AuthLayer;

impl<S> Layer<S> for AuthLayer {
    type Service = Auth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Auth { inner }
    }
}

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
