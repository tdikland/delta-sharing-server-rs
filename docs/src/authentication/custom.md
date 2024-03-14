# Custom authentication middleware

Like other areas of the Delta Sharing server, it is possible to extend the server by implementing your own authentication middleware.

## How is authentication handled?

The handlers for all of the routes in the Delta Sharing protocol expect a request extension with the `ClientId`. If this extension is not set, the handler will return an error response.
The `ClientId` is the type that identifies the client that is calling the server (or is set to `ClientId::Anonymous` if the client could/should not be identified).

## Example

An example of custom middleware can be found below. In this example the middleware 

```rust
const SUPER_SECRET_PASSWORD: &str = "delta-sharing";

async fn auth_middleware(mut request: Request, next: Next) -> Result<Response, ServerError> {
    if let Some(token) = request.headers().get(AUTHORIZATION) {
        let token = token.to_str().unwrap();
        if token == SUPER_SECRET_PASSWORD {
            let client_id = ClientId::anonymous();
            tracing::info!(client_id=%client_id, "authenticated");
            request.extensions_mut().insert(client_id);
            let response = next.run(request).await;
            return Ok(response);
        }
    }

    Err(ServerError::unauthorized(""))
}

let mut state = SharingServerState::new(...);
let svc = build_sharing_server_router(Arc::new(state));

let app = svc
    .layer(middleware::from_fn(auth_middleware));

let listener = TcpListener::bind("127.0.0.1:0")
    .await
    .expect("Could not bind to socket");
let addr = listener.local_addr().unwrap();
tokio::spawn(async move {
    axum::serve(listener, app).await.expect("server error");
});
```


