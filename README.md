# bounded-axum-server

A minimal, runnable reference showing how to keep the high-level **Axum** API (`Router`, extractors, `IntoResponse`, Tower middleware) everywhere except a single ~40-line **Hyper HTTP/1.1** accept loop, while enforcing provable ceilings on file descriptors, concurrency, request memory, and stalled clients.

*(Uses `tokio::net::TcpListener`; swapping to `UnixListener` is a one-line change since `TokioIo::new(stream)` works on any `AsyncRead + AsyncWrite`.)*

## Architecture

- **Routes (`src/lib.rs`)**: Plain `axum::Router` handlers. Nothing in route modules knows about Hyper or connection management.
- **Server (`src/server.rs`)**:
  - **Request-level middleware** (applied to the `Router`):
    - `HandleErrorLayer` + `tower::load_shed::LoadShedLayer` + `tower::limit::ConcurrencyLimitLayer` — caps concurrent in-flight requests and immediately returns `503 Service Unavailable` when full.
    - `tower_http::timeout::TimeoutLayer` — cancels slow request bodies and slow handlers and returns `408 Request Timeout`.
    - `tower_http::limit::RequestBodyLimitLayer` (+ `DefaultBodyLimit::disable()`) — enforces `max_body_bytes` uniformly across every route and extractor (including raw `Body` streams) and rejects oversized `Content-Length` upfront with `413 Payload Too Large`.
  - **Connection-level controls** (Hyper `http1::Builder` accept loop):
    - `Arc<Semaphore>` sized structurally as `max_connections = max_concurrent_requests + shed_connection_headroom`, so `max_connections` can never be misconfigured below `max_concurrent_requests`. Acquires a permit before `accept()`, bounding open FDs and spawned tasks.
    - Explicit `hyper::server::conn::http1::Builder` — guarantees HTTP/1.1-only even if `axum/http2` is enabled elsewhere in a workspace via feature unification.
    - `keep_alive(false)` — one request per connection; eliminates idle keep-alive slot squatting.
    - `header_read_timeout` with `TokioTimer` — drops connect-and-stall / slowloris clients before headers complete.
    - `max_buf_size` — bounds per-connection HTTP header buffer allocation (`431 Request Header Fields Too Large`).
    - `hyper_util::server::graceful::GracefulShutdown` bounded by `shutdown_drain_timeout`.

Worst-case memory across clients is bounded by:

```text
max_connections × max_header_buf_bytes  +  max_concurrent_requests × max_body_bytes
```

Worst-case lifetime of any single connection (for buffered responses) is bounded by:

```text
header_read_timeout + request_timeout
```

### Caveats

- **Pre-header vs. post-header `408` format**: `header_read_timeout` fires inside Hyper before Tower middleware runs, so it closes the socket or writes a bare HTTP `408` rather than a JSON error body.
- **Streaming responses**: `tower_http::timeout::TimeoutLayer` bounds reading the request body and producing the `Response`, not draining a streaming response body to a slow reader after headers are sent. For buffered JSON responses that is exact; if streaming/SSE routes are added later, add a write/idle deadline on the stream or socket.

## Proving Every Limit

Run the integration test suite (`tests/limits.rs`):

```sh
cargo test
```

| Limit | Mechanism | Proven by |
| --- | --- | --- |
| Keep-alive disabled | `http1::Builder::keep_alive(false)` | `hello_and_echo_succeed_and_disable_keep_alive` |
| Request concurrency + load shedding (`503`) | `LoadShedLayer` + `ConcurrencyLimitLayer` | `excess_concurrent_requests_return_503_immediately` |
| Open connections / FDs | Pre-accept `Semaphore` (`max_concurrent_requests + shed_connection_headroom`) | `connection_cap_bounds_accepted_sockets_and_unblocks_when_slot_frees` |
| Silent connect-and-stall | `header_read_timeout` | `stalled_client_sending_zero_bytes_is_dropped_by_header_timeout` |
| Partial-header slowloris | `header_read_timeout` | `partial_headers_slowloris_is_dropped_by_header_timeout` |
| Slow handler (`408`) | `TimeoutLayer` | `slow_handler_returns_408_and_frees_concurrency_slot` |
| Slow body (`408`) | `TimeoutLayer` | `slow_body_returns_408_and_closes_connection` |
| Body size (`413`) — `Content-Length`, chunked, raw `Body` | `RequestBodyLimitLayer` | `request_body_limit_enforced_for_content_length_chunked_and_raw_body` |
| Header buffer size (`431`) | `http1::Builder::max_buf_size` | `oversized_headers_rejected_by_max_buf_size` |
| Graceful shutdown drain | `GracefulShutdown` | `graceful_shutdown_drains_in_flight_request_before_exiting` |
| Shutdown drain cap | `timeout(shutdown_drain_timeout, graceful.shutdown())` | `graceful_shutdown_respects_drain_timeout_for_wedged_request` |
| Config invariants | `ServerLimits::new` | `limits_validation_rejects_invalid_configurations` |

## Running Manually

```sh
cargo run -- 127.0.0.1:3000
# In another terminal:
curl http://127.0.0.1:3000/hello
```
