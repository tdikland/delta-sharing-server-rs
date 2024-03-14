//! Authentication middleware for public access.

use std::task::{Context, Poll};

use axum::extract::Request;
use tower::{Layer, Service};

use crate::auth::RecipientId;

/// Authentication middleware.
///
/// Does not perform any authentication, but sets the client identifier to anonymous.
#[derive(Debug, Clone)]
pub struct PublicAccessAuthLayer;

impl PublicAccessAuthLayer {
    /// Create a new public access authentication layer.
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for PublicAccessAuthLayer {
    type Service = PublicAccessAuth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PublicAccessAuth { inner }
    }
}

/// Authentication middleware.
///
/// Does not perform any authentication, but sets the client identifier to anonymous.
#[derive(Debug, Clone)]
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
        let client_id = RecipientId::unknown();
        tracing::info!(client_id=%client_id, "authenticated");

        req.extensions_mut().insert(client_id);
        self.inner.call(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::response::Response;
    use http::{header, Request, StatusCode};
    use tower::BoxError;
    use tower::ServiceBuilder;
    use tower::ServiceExt;

    #[tokio::test]
    async fn public_access_auth_with_bearer() {
        let mut service = ServiceBuilder::new()
            .layer(PublicAccessAuthLayer::new())
            .service_fn(check_recipient);

        let request = Request::get("/")
            .header(header::AUTHORIZATION, "Bearer Foobar")
            .body(Body::empty())
            .unwrap();
        let res = service.ready().await.unwrap().call(request).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn public_access_auth_without_bearer() {
        let mut service = ServiceBuilder::new()
            .layer(PublicAccessAuthLayer::new())
            .service_fn(check_recipient);

        let request = Request::get("/").body(Body::empty()).unwrap();
        let res = service.ready().await.unwrap().call(request).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    async fn check_recipient(req: Request<Body>) -> Result<Response<Body>, BoxError> {
        assert_eq!(
            req.extensions().get::<RecipientId>(),
            Some(&RecipientId::Anonymous)
        );
        Ok(Response::new(req.into_body()))
    }
}
