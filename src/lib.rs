//! Reference bounded HTTP/1.1 web server built with Axum, Hyper, and Tower.
//!
//! Demonstrates how to keep the high-level Axum routing/extractor API everywhere except the
//! single [`server::serve`] accept loop, while enforcing provable ceilings on:
//!
//! 1. **In-flight request concurrency** (`tower::limit::ConcurrencyLimitLayer` +
//!    `tower::load_shed::LoadShedLayer` → `503 Service Unavailable`).
//! 2. **Simultaneous open connections / file descriptors** (`tokio::sync::Semaphore` sized
//!    structurally from `max_concurrent_requests + shed_connection_headroom`).
//! 3. **Idle / stalled-before-headers clients** (`hyper::server::conn::http1::Builder::header_read_timeout`
//!    + `keep_alive(false)`).
//! 4. **Slow request bodies and slow handlers** (`tower_http::timeout::TimeoutLayer` →
//!    `408 Request Timeout`).
//! 5. **Request body size** (`tower_http::limit::RequestBodyLimitLayer` →
//!    `413 Payload Too Large`, enforced uniformly across all routes and extractors).
//! 6. **HTTP/1.1 header buffer size** (`hyper::server::conn::http1::Builder::max_buf_size` →
//!    `431 Request Header Fields Too Large`).
//! 7. **Graceful shutdown drain time** (`hyper_util::server::graceful::GracefulShutdown`
//!    bounded by `shutdown_drain_timeout`).

pub mod server;

use axum::body::Bytes;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

/// Response payload for `GET /hello`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloResponse {
    /// Greeting text.
    pub message: String,
}

/// Example high-level Axum router. Nothing in route modules needs to know about Hyper or the
/// connection loop.
pub fn hello_router() -> Router {
    Router::new()
        .route(
            "/hello",
            get(|| async {
                Json(HelloResponse {
                    message: "hello, world".to_owned(),
                })
            }),
        )
        .route("/echo", post(|body: Bytes| async move { body }))
}
