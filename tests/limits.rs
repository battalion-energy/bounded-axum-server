//! Integration tests that deterministically prove every limit enforced by `bounded_axum_server`.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Router;
use bounded_axum_server::hello_router;
use bounded_axum_server::server::{self, LimitsError, ServerLimits, MIN_HEADER_BUF_BYTES};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Barrier, Notify};
use tokio::task::JoinHandle;

struct TestServer {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn start(router: Router, limits: ServerLimits) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback TCP listener");
        let addr = listener.local_addr().expect("local addr");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let task = tokio::spawn(async move {
            server::serve(listener, router, limits, async move {
                let _ = shutdown_rx.await;
            })
            .await;
        });

        Self {
            addr,
            shutdown_tx: Some(shutdown_tx),
            task,
        }
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn trigger_shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }

    async fn shutdown(mut self) {
        self.trigger_shutdown();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("server shutdown timed out")
            .expect("server task panicked");
    }
}

fn fast_limits(
    max_concurrent: usize,
    shed_headroom: usize,
    max_body_bytes: usize,
    header_read_timeout: Duration,
    request_timeout: Duration,
    shutdown_drain_timeout: Duration,
) -> ServerLimits {
    ServerLimits::new(
        NonZeroUsize::new(max_concurrent).expect("non-zero"),
        shed_headroom,
        max_body_bytes,
        MIN_HEADER_BUF_BYTES,
        header_read_timeout,
        request_timeout,
        shutdown_drain_timeout,
    )
    .expect("valid test limits")
}

/// Send raw HTTP/1.1 bytes over TCP and read until EOF, returning `(status_code, raw_headers, body)`.
async fn raw_request(addr: SocketAddr, request: &[u8]) -> (u16, String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(request).await.expect("write request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read response");
    parse_http_response(&response)
}

fn parse_http_response(raw: &[u8]) -> (u16, String, String) {
    let text = String::from_utf8(raw.to_vec()).expect("valid utf-8 response");
    let (head, body) = text
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed HTTP response: {text:?}"));
    let status_line = head.lines().next().expect("status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status code");
    (status, head.to_owned(), body.to_owned())
}

#[tokio::test]
async fn hello_and_echo_succeed_and_disable_keep_alive() {
    let server = TestServer::start(hello_router(), ServerLimits::default()).await;

    let mut stream = TcpStream::connect(server.addr()).await.unwrap();
    stream
        .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let (status, headers, body) = parse_http_response(&response);
    assert_eq!(status, 200);
    assert!(
        headers.to_ascii_lowercase().contains("connection: close"),
        "expected keep-alive disabled (`connection: close`), got:\n{headers}"
    );
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["message"], "hello, world");

    // A second read on the same connection must see EOF immediately (no keep-alive reuse).
    let mut extra = [0u8; 1];
    assert_eq!(stream.read(&mut extra).await.unwrap(), 0);

    // POST /echo returns the exact payload.
    let (status, _, body) = raw_request(
        server.addr(),
        b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nping!",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, "ping!");

    server.shutdown().await;
}

#[tokio::test]
async fn excess_concurrent_requests_return_503_immediately() {
    let entered = Arc::new(Barrier::new(3)); // 2 handlers + test task
    let release = Arc::new(Notify::new());

    let router = hello_router().route(
        "/block",
        get({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || async move {
                entered.wait().await;
                release.notified().await;
                StatusCode::OK
            }
        }),
    );

    let limits = fast_limits(
        2, // max_concurrent_requests
        2, // shed_connection_headroom -> 4 max connections
        1024,
        Duration::from_secs(2),
        Duration::from_secs(5),
        Duration::from_secs(2),
    );
    let server = TestServer::start(router, limits).await;

    let req1 = tokio::spawn({
        let addr = server.addr();
        async move { raw_request(addr, b"GET /block HTTP/1.1\r\nHost: localhost\r\n\r\n").await }
    });
    let req2 = tokio::spawn({
        let addr = server.addr();
        async move { raw_request(addr, b"GET /block HTTP/1.1\r\nHost: localhost\r\n\r\n").await }
    });

    // Wait until both concurrency slots are definitely occupied inside `/block`.
    entered.wait().await;

    // A 3rd request within connection headroom must be shed immediately with 503.
    let (status, _, body) = tokio::time::timeout(
        Duration::from_secs(1),
        raw_request(
            server.addr(),
            b"GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n",
        ),
    )
    .await
    .expect("503 should arrive immediately, not block");
    assert_eq!(status, 503, "body: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["error"], "overloaded");

    // Release the blocked requests; both finish 200 and free their permits.
    release.notify_waiters();
    assert_eq!(req1.await.unwrap().0, 200);
    assert_eq!(req2.await.unwrap().0, 200);

    // Subsequent request succeeds again.
    let (status, _, _) = raw_request(
        server.addr(),
        b"GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert_eq!(status, 200);

    server.shutdown().await;
}

#[tokio::test]
async fn connection_cap_bounds_accepted_sockets_and_unblocks_when_slot_frees() {
    let entered = Arc::new(Barrier::new(3)); // 2 handlers + test task
    let release = Arc::new(Notify::new());

    let router = hello_router().route(
        "/block",
        get({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || async move {
                entered.wait().await;
                release.notified().await;
                StatusCode::OK
            }
        }),
    );

    // Zero shed headroom: `max_connections == 2`, so a 3rd client cannot even be accepted
    // (nor handed a 503) while both connection slots are occupied.
    let limits = fast_limits(
        2, // max_concurrent_requests
        0, // shed_connection_headroom -> 2 max open connections
        1024,
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(2),
    );
    let server = TestServer::start(router, limits).await;

    let req1 = tokio::spawn({
        let addr = server.addr();
        async move { raw_request(addr, b"GET /block HTTP/1.1\r\nHost: localhost\r\n\r\n").await }
    });
    let req2 = tokio::spawn({
        let addr = server.addr();
        async move { raw_request(addr, b"GET /block HTTP/1.1\r\nHost: localhost\r\n\r\n").await }
    });

    // Deterministically prove both connections are accepted and occupying both permits.
    entered.wait().await;

    // A 3rd client must wait because `accept()` is paused at `max_connections` (2).
    let mut third = TcpStream::connect(server.addr()).await.unwrap();
    third
        .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    let mut buf = Vec::new();
    let blocked =
        tokio::time::timeout(Duration::from_millis(120), third.read_to_end(&mut buf)).await;
    assert!(
        blocked.is_err(),
        "3rd connection should wait while all connection slots are occupied"
    );

    // Releasing one in-flight request closes its connection and frees a permit; client #3 gets 200.
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), third.read_to_end(&mut buf))
        .await
        .expect("3rd client should be served once a slot frees")
        .unwrap();
    let (status, _, _) = parse_http_response(&buf);
    assert_eq!(status, 200);

    release.notify_waiters();
    assert_eq!(req1.await.unwrap().0, 200);
    assert_eq!(req2.await.unwrap().0, 200);

    server.shutdown().await;
}

#[tokio::test]
async fn stalled_client_sending_zero_bytes_is_dropped_by_header_timeout() {
    let header_timeout = Duration::from_millis(120);
    let limits = fast_limits(
        1,
        0, // max_connections = 1 so a leaked slot would deadlock the next client
        1024,
        header_timeout,
        Duration::from_secs(5),
        Duration::from_secs(2),
    );
    let server = TestServer::start(hello_router(), limits).await;

    // Connect and send NOTHING at all.
    let mut silent = TcpStream::connect(server.addr()).await.unwrap();
    let start = Instant::now();
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(2), silent.read(&mut buf))
        .await
        .expect("server must close silent connection on header_read_timeout")
        .unwrap();
    assert_eq!(n, 0, "expected EOF after header timeout");
    assert!(
        start.elapsed() >= Duration::from_millis(80),
        "closed too early ({:?})",
        start.elapsed()
    );

    // Because the silent client was dropped, the single connection slot is free again.
    let (status, _, _) = raw_request(
        server.addr(),
        b"GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert_eq!(status, 200);

    server.shutdown().await;
}

#[tokio::test]
async fn partial_headers_slowloris_is_dropped_by_header_timeout() {
    let limits = fast_limits(
        1,
        0,
        1024,
        Duration::from_millis(120),
        Duration::from_secs(5),
        Duration::from_secs(2),
    );
    let server = TestServer::start(hello_router(), limits).await;

    // Send incomplete headers (no terminating CRLFCRLF) and stall.
    let mut slow = TcpStream::connect(server.addr()).await.unwrap();
    slow.write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();

    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), slow.read_to_end(&mut reply))
        .await
        .expect("server must close slowloris connection")
        .unwrap();

    // Server either closes immediately or sends 408 then closes; either way the slot is freed.
    if !reply.is_empty() {
        let (status, _, _) = parse_http_response(&reply);
        assert_eq!(status, 408);
    }

    let (status, _, _) = raw_request(
        server.addr(),
        b"GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert_eq!(status, 200);

    server.shutdown().await;
}

#[tokio::test]
async fn slow_handler_returns_408_and_frees_concurrency_slot() {
    let router = hello_router().route(
        "/hang",
        get(|| async {
            std::future::pending::<()>().await;
            StatusCode::OK
        }),
    );

    let limits = fast_limits(
        1, // single concurrency slot
        1,
        1024,
        Duration::from_secs(2),
        Duration::from_millis(120),
        Duration::from_secs(2),
    );
    let server = TestServer::start(router, limits).await;

    let (status, _, _) = tokio::time::timeout(
        Duration::from_secs(2),
        raw_request(
            server.addr(),
            b"GET /hang HTTP/1.1\r\nHost: localhost\r\n\r\n",
        ),
    )
    .await
    .expect("request timeout should fire");
    assert_eq!(status, 408);

    // Concurrency slot must be free immediately after the 408.
    let (status, _, _) = raw_request(
        server.addr(),
        b"GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert_eq!(status, 200);

    server.shutdown().await;
}

#[tokio::test]
async fn slow_body_returns_408_and_closes_connection() {
    let limits = fast_limits(
        1,
        0,
        1024,
        Duration::from_secs(2),
        Duration::from_millis(120),
        Duration::from_secs(2),
    );
    let server = TestServer::start(hello_router(), limits).await;

    // Declare Content-Length: 16, send only 2 bytes, then stall.
    let mut stream = TcpStream::connect(server.addr()).await.unwrap();
    stream
        .write_all(b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 16\r\n\r\nhi")
        .await
        .unwrap();

    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut reply))
        .await
        .expect("server must time out stalled request body")
        .unwrap();
    let (status, _, _) = parse_http_response(&reply);
    assert_eq!(status, 408);

    // Single slot is freed and healthy client succeeds.
    let (status, _, _) = raw_request(
        server.addr(),
        b"GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert_eq!(status, 200);

    server.shutdown().await;
}

#[tokio::test]
async fn request_body_limit_enforced_for_content_length_chunked_and_raw_body() {
    // Include a handler that reads raw `axum::body::Body` directly (bypassing Axum's
    // DefaultBodyLimit-aware extractors) to prove RequestBodyLimitLayer covers every route.
    let router = hello_router().route(
        "/raw",
        post(|body: Body| async move {
            match axum::body::to_bytes(body, usize::MAX).await {
                Ok(bytes) => (StatusCode::OK, bytes),
                Err(_) => (StatusCode::PAYLOAD_TOO_LARGE, axum::body::Bytes::new()),
            }
        }),
    );

    let limits = fast_limits(
        4,
        2,
        64, // max_body_bytes
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );
    let server = TestServer::start(router, limits).await;

    // 1. Exact limit (64 bytes) succeeds.
    let ok_body = "a".repeat(64);
    let req =
        format!("POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 64\r\n\r\n{ok_body}");
    let (status, _, body) = raw_request(server.addr(), req.as_bytes()).await;
    assert_eq!(status, 200);
    assert_eq!(body.len(), 64);

    // 2. Over limit via Content-Length (65 bytes) is rejected upfront with 413 even before body bytes arrive.
    let req = "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 65\r\n\r\n";
    let (status, _, _) = raw_request(server.addr(), req.as_bytes()).await;
    assert_eq!(status, 413);

    // 3. Over limit via chunked transfer encoding (no Content-Length) is rejected with 413.
    let chunk = "b".repeat(65);
    let req = format!(
        "POST /echo HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n41\r\n{chunk}\r\n0\r\n\r\n"
    );
    let (status, _, _) = raw_request(server.addr(), req.as_bytes()).await;
    assert_eq!(status, 413);

    // 4. Raw `axum::body::Body` route is also rejected with 413 by RequestBodyLimitLayer.
    let req = format!(
        "POST /raw HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n41\r\n{chunk}\r\n0\r\n\r\n"
    );
    let (status, _, _) = raw_request(server.addr(), req.as_bytes()).await;
    assert_eq!(status, 413);

    server.shutdown().await;
}

#[tokio::test]
async fn oversized_headers_rejected_by_max_buf_size() {
    let limits = fast_limits(
        2,
        2,
        1024,
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );
    let server = TestServer::start(hello_router(), limits).await;

    // Exceeds MIN_HEADER_BUF_BYTES (8 KiB).
    let huge_header = "x".repeat(MIN_HEADER_BUF_BYTES + 1024);
    let req = format!("GET /hello HTTP/1.1\r\nHost: localhost\r\nX-Pad: {huge_header}\r\n\r\n");

    let mut stream = TcpStream::connect(server.addr()).await.unwrap();
    let _ = stream.write_all(req.as_bytes()).await;
    let mut reply = Vec::new();
    let _ = stream.read_to_end(&mut reply).await;

    assert!(
        !reply.is_empty(),
        "expected HTTP 431 response for oversized headers"
    );
    let (status, _, _) = parse_http_response(&reply);
    assert_eq!(status, 431);

    server.shutdown().await;
}

#[tokio::test]
async fn graceful_shutdown_drains_in_flight_request_before_exiting() {
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Notify::new());

    let router = hello_router().route(
        "/block",
        get({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || async move {
                entered.wait().await;
                release.notified().await;
                "finished"
            }
        }),
    );

    let limits = fast_limits(
        2,
        2,
        1024,
        Duration::from_secs(2),
        Duration::from_secs(5),
        Duration::from_secs(2),
    );
    let mut server = TestServer::start(router, limits).await;

    let req = tokio::spawn({
        let addr = server.addr();
        async move { raw_request(addr, b"GET /block HTTP/1.1\r\nHost: localhost\r\n\r\n").await }
    });

    entered.wait().await;
    server.trigger_shutdown();

    // Release the in-flight request after shutdown started; it must still complete with 200.
    tokio::time::sleep(Duration::from_millis(30)).await;
    release.notify_one();

    let (status, _, body) = req.await.unwrap();
    assert_eq!(status, 200);
    assert_eq!(body, "finished");

    tokio::time::timeout(Duration::from_secs(2), server.task)
        .await
        .expect("server should exit once in-flight request completes")
        .unwrap();
}

#[tokio::test]
async fn graceful_shutdown_respects_drain_timeout_for_wedged_request() {
    let entered = Arc::new(Barrier::new(2));

    let router = hello_router().route(
        "/hang",
        get({
            let entered = Arc::clone(&entered);
            move || async move {
                entered.wait().await;
                std::future::pending::<()>().await;
                StatusCode::OK
            }
        }),
    );

    let drain_timeout = Duration::from_millis(120);
    let limits = fast_limits(
        2,
        2,
        1024,
        Duration::from_secs(2),
        Duration::from_secs(30), // request_timeout longer than shutdown_drain_timeout
        drain_timeout,
    );
    let mut server = TestServer::start(router, limits).await;

    let _client = tokio::spawn({
        let addr = server.addr();
        async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            let _ = s
                .write_all(b"GET /hang HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await;
            let mut buf = Vec::new();
            let _ = s.read_to_end(&mut buf).await;
        }
    });

    entered.wait().await;
    let start = Instant::now();
    server.trigger_shutdown();

    tokio::time::timeout(Duration::from_secs(2), server.task)
        .await
        .expect("shutdown must not hang past shutdown_drain_timeout")
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "shutdown took {:?}, expected ~{drain_timeout:?}",
        start.elapsed()
    );
}

#[test]
fn limits_validation_rejects_invalid_configurations() {
    let nz = NonZeroUsize::new(4).unwrap();
    let d = Duration::from_secs(1);

    assert_eq!(
        ServerLimits::new(nz, 4, 1024, MIN_HEADER_BUF_BYTES - 1, d, d, d),
        Err(LimitsError::HeaderBufTooSmall {
            actual: MIN_HEADER_BUF_BYTES - 1
        })
    );
    assert_eq!(
        ServerLimits::new(nz, 4, 1024, MIN_HEADER_BUF_BYTES, Duration::ZERO, d, d),
        Err(LimitsError::ZeroDuration {
            name: "header_read_timeout"
        })
    );
    assert_eq!(
        ServerLimits::new(nz, 4, 1024, MIN_HEADER_BUF_BYTES, d, Duration::ZERO, d),
        Err(LimitsError::ZeroDuration {
            name: "request_timeout"
        })
    );
    assert_eq!(
        ServerLimits::new(nz, 4, 1024, MIN_HEADER_BUF_BYTES, d, d, Duration::ZERO),
        Err(LimitsError::ZeroDuration {
            name: "shutdown_drain_timeout"
        })
    );
    assert_eq!(
        ServerLimits::new(nz, usize::MAX, 1024, MIN_HEADER_BUF_BYTES, d, d, d),
        Err(LimitsError::ConnectionLimitOverflow)
    );
}
