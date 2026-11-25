//! Bounded HTTP/1.1 web server using Axum for routing/middleware and Hyper's HTTP/1.1
//! connection builder as the escape hatch for connection-level limits.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use axum::extract::DefaultBodyLimit;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{BoxError, Json, Router};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use serde::Serialize;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tower::limit::ConcurrencyLimitLayer;
use tower::load_shed::LoadShedLayer;
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;

/// Minimum `max_buf_size` accepted by Hyper's HTTP/1.1 parser (Hyper panics below 8 KiB).
pub const MIN_HEADER_BUF_BYTES: usize = 8 * 1024;

/// Validation error returned by [`ServerLimits::new`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LimitsError {
    /// `max_header_buf_bytes` was smaller than Hyper's 8 KiB minimum.
    #[error("max_header_buf_bytes ({actual}) must be at least {MIN_HEADER_BUF_BYTES}")]
    HeaderBufTooSmall {
        /// Supplied value.
        actual: usize,
    },
    /// A timeout duration was zero.
    #[error("{name} must be greater than zero")]
    ZeroDuration {
        /// Field name.
        name: &'static str,
    },
    /// `max_concurrent_requests + shed_connection_headroom` overflowed `usize`.
    #[error("max_concurrent_requests + shed_connection_headroom overflows usize")]
    ConnectionLimitOverflow,
}

/// Structurally consistent resource limits for [`serve`].
///
/// `max_connections` is derived from `max_concurrent_requests + shed_connection_headroom` so the
/// connection cap can never be configured below the request concurrency cap (which would prevent
/// `503 Service Unavailable` load-shedding from ever firing).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerLimits {
    /// Hard cap on HTTP requests executing concurrently across all connections.
    /// Excess requests arriving while all slots are busy are immediately rejected with `503`.
    max_concurrent_requests: NonZeroUsize,
    /// Additional accepted connection slots above `max_concurrent_requests` so overload clients
    /// can be accepted, sent `503 Service Unavailable`, and closed instead of stalling in the
    /// kernel backlog.
    shed_connection_headroom: usize,
    /// Hard cap on request body size in bytes (`413 Payload Too Large`).
    max_body_bytes: usize,
    /// Hard cap on Hyper's HTTP/1.1 read buffer for request headers (`>= 8 KiB`).
    max_header_buf_bytes: usize,
    /// Maximum time after connect for the client to finish sending HTTP headers.
    /// Enforced by Hyper before Tower middleware runs (so a timeout closes the socket or sends a
    /// bare `408`, not a JSON error body).
    header_read_timeout: Duration,
    /// Maximum time for reading the request body and producing the HTTP response (`408`).
    /// Covers buffered replies end-to-end; does not bound draining a streaming response body to a
    /// slow reader after headers are sent.
    request_timeout: Duration,
    /// Maximum time graceful shutdown waits for in-flight connections to finish.
    shutdown_drain_timeout: Duration,
}

impl ServerLimits {
    /// Construct validated limits.
    pub fn new(
        max_concurrent_requests: NonZeroUsize,
        shed_connection_headroom: usize,
        max_body_bytes: usize,
        max_header_buf_bytes: usize,
        header_read_timeout: Duration,
        request_timeout: Duration,
        shutdown_drain_timeout: Duration,
    ) -> Result<Self, LimitsError> {
        if max_header_buf_bytes < MIN_HEADER_BUF_BYTES {
            return Err(LimitsError::HeaderBufTooSmall {
                actual: max_header_buf_bytes,
            });
        }
        if header_read_timeout.is_zero() {
            return Err(LimitsError::ZeroDuration {
                name: "header_read_timeout",
            });
        }
        if request_timeout.is_zero() {
            return Err(LimitsError::ZeroDuration {
                name: "request_timeout",
            });
        }
        if shutdown_drain_timeout.is_zero() {
            return Err(LimitsError::ZeroDuration {
                name: "shutdown_drain_timeout",
            });
        }
        if max_concurrent_requests
            .get()
            .checked_add(shed_connection_headroom)
            .is_none()
        {
            return Err(LimitsError::ConnectionLimitOverflow);
        }

        Ok(Self {
            max_concurrent_requests,
            shed_connection_headroom,
            max_body_bytes,
            max_header_buf_bytes,
            header_read_timeout,
            request_timeout,
            shutdown_drain_timeout,
        })
    }

    /// Maximum concurrently executing HTTP requests.
    pub fn max_concurrent_requests(&self) -> NonZeroUsize {
        self.max_concurrent_requests
    }

    /// Maximum simultaneously open connections (file descriptors / connection tasks).
    /// Always `>= max_concurrent_requests`.
    pub fn max_connections(&self) -> usize {
        self.max_concurrent_requests.get() + self.shed_connection_headroom
    }

    /// Maximum request body size in bytes.
    pub fn max_body_bytes(&self) -> usize {
        self.max_body_bytes
    }

    /// Maximum HTTP/1.1 header buffer size in bytes.
    pub fn max_header_buf_bytes(&self) -> usize {
        self.max_header_buf_bytes
    }

    /// Maximum time allowed for a client to send complete HTTP headers.
    pub fn header_read_timeout(&self) -> Duration {
        self.header_read_timeout
    }

    /// Maximum time allowed for reading the request body and completing the handler.
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Maximum time graceful shutdown waits for open connections to drain.
    pub fn shutdown_drain_timeout(&self) -> Duration {
        self.shutdown_drain_timeout
    }
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_concurrent_requests: NonZeroUsize::new(8).expect("8 is non-zero"),
            shed_connection_headroom: 8,
            max_body_bytes: 256 * 1024,
            max_header_buf_bytes: 16 * 1024,
            header_read_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            shutdown_drain_timeout: Duration::from_secs(5),
        }
    }
}

/// Serve an Axum [`Router`] over TCP with all [`ServerLimits`] enforced.
///
/// The high-level Axum router is wrapped with Tower / Tower-HTTP middleware for request-level
/// controls (`503` load shedding, `408` request timeouts, `413` body limits), and served through
/// Hyper's HTTP/1.1 builder directly so connection-level limits (`header_read_timeout`,
/// `keep_alive(false)`, `max_buf_size`, explicit HTTP/1.1-only, and bounded graceful shutdown)
/// are enforced.
pub async fn serve(
    listener: TcpListener,
    router: Router,
    limits: ServerLimits,
    shutdown: impl Future<Output = ()>,
) {
    let app = apply_request_limits(router, limits);
    let connection_slots = Arc::new(Semaphore::new(limits.max_connections()));
    let graceful = GracefulShutdown::new();

    tokio::pin!(shutdown);

    loop {
        // Acquire a connection slot before calling `accept()` so open FDs and spawned tasks
        // never exceed `limits.max_connections()`. Selecting against `shutdown` here guarantees
        // shutdown begins promptly even if all connection slots are currently occupied.
        let permit = tokio::select! {
            biased;
            () = &mut shutdown => break,
            permit = Arc::clone(&connection_slots).acquire_owned() => match permit {
                Ok(permit) => permit,
                // Defensive: `connection_slots` is never closed, so this is unreachable.
                Err(_) => break,
            },
        };

        let stream = tokio::select! {
            biased;
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _addr)) => stream,
                Err(err) => {
                    tracing::warn!(%err, "failed to accept TCP connection");
                    continue;
                }
            },
        };

        // Explicit HTTP/1.1-only connection with `keep_alive(false)`:
        // - Immune to Cargo feature unification enabling `axum/http2` elsewhere.
        // - Eliminates idle keep-alive slot squatting.
        // - Bounds worst-case connection lifetime to `header_read_timeout + request_timeout`
        //   for buffered responses.
        let mut http = http1::Builder::new();
        http.timer(TokioTimer::new())
            .header_read_timeout(limits.header_read_timeout())
            .keep_alive(false)
            .max_buf_size(limits.max_header_buf_bytes());

        let service = TowerToHyperService::new(app.clone());
        let conn = graceful.watch(http.serve_connection(TokioIo::new(stream), service));

        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = conn.await {
                tracing::debug!(%err, "connection closed with error");
            }
        });
    }

    // Wait for in-flight connections to finish, bounded by `shutdown_drain_timeout`.
    if tokio::time::timeout(limits.shutdown_drain_timeout(), graceful.shutdown())
        .await
        .is_err()
    {
        tracing::warn!(
            timeout = ?limits.shutdown_drain_timeout(),
            "shutdown drain timed out with connections still open"
        );
    }
}

/// Apply request-level middleware limits around an Axum [`Router`].
///
/// `ConcurrencyLimitLayer` (whose internal `PollSemaphore` is `!Sync`) wraps the `Router` from
/// outside rather than via `Router::layer`, avoiding `Router::layer`'s `Sync` bound and needing
/// no `BufferLayer` background task. Every clone of the returned service shares the same
/// underlying `Arc<Semaphore>`.
///
/// Because `axum::Router<()>` implements `Service<Request<B>>` for any `HttpBody<Data = Bytes>`
/// (including Hyper's `Incoming`) and converts it to `axum::body::Body` internally, Hyper plugs
/// straight into the returned service through `TowerToHyperService` without a `map_request` layer.
fn apply_request_limits(
    router: Router,
    limits: ServerLimits,
) -> impl tower::Service<
    Request<Incoming>,
    Response = Response,
    Error = std::convert::Infallible,
    Future = impl Future<Output = Result<Response, std::convert::Infallible>> + Send,
> + Clone
       + Send
       + 'static {
    let router = router
        // Disable Axum's 2 MiB extractor-only default so RequestBodyLimitLayer is the single
        // source of truth for every route and extractor (including raw streaming bodies).
        .layer(DefaultBodyLimit::disable())
        // Enforce body size unconditionally across all routes/extractors (413 Payload Too Large).
        .layer(RequestBodyLimitLayer::new(limits.max_body_bytes()))
        // Cancel slow body reads or slow handlers and return 408 Request Timeout.
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            limits.request_timeout(),
        ));

    ServiceBuilder::new()
        // Outermost: convert LoadShed's `Overloaded` error into `503 Service Unavailable`.
        .layer(HandleErrorLayer::new(handle_middleware_error))
        // Immediately shed excess requests instead of queueing them in memory.
        .layer(LoadShedLayer::new())
        // Hard cap on concurrent in-flight requests (shared Arc<Semaphore> across clones).
        .layer(ConcurrencyLimitLayer::new(
            limits.max_concurrent_requests().get(),
        ))
        .service(router)
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    message: String,
}

async fn handle_middleware_error(err: BoxError) -> Response {
    if err.is::<tower::load_shed::error::Overloaded>() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorBody {
                error: "overloaded",
                message: "server at request concurrency limit".to_owned(),
            }),
        )
            .into_response();
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: "internal_error",
            message: err.to_string(),
        }),
    )
        .into_response()
}
