//! Authentication middleware for public access.

use std::task::{Context, Poll};

use axum::extract::Request;
use tower::{Layer, Service};

use crate::auth::ClientId;

/// Authentication middleware.
///
/// Does not perform any authentication, but sets the client identifier to anonymous.
#[derive(Clone)]
pub struct PublicAccessAuthLayer;

impl<S> Layer<S> for PublicAccessAuthLayer {
    type Service = PublicAccessAuth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PublicAccessAuth { inner }
    }
}

/// Authentication middleware.
#[derive(Clone)]
pub struct PublicAccessAuth<S> {
    inner: S,
}

impl<S> Service<Request> for PublicAccessAuth<S>
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
        let client_id = ClientId::unknown();
        tracing::info!(client_id=%client_id, "authenticated");
        req.extensions_mut().insert(client_id);
        self.inner.call(req)
    }
}
