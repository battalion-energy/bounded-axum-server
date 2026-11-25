//! Runs the hello-world bounded Axum server on a Unix socket path (default: `/tmp/bounded-axum.sock`).

use std::path::PathBuf;

use bounded_axum_server::hello_router;
use bounded_axum_server::server::{self, ServerLimits};
use tokio::net::UnixListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let socket_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/bounded-axum.sock"));

    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    tracing::info!(socket = %socket_path.display(), "listening");

    server::serve(listener, hello_router(), ServerLimits::default(), async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutting down");
    })
    .await;

    let _ = std::fs::remove_file(&socket_path);
    Ok(())
}
