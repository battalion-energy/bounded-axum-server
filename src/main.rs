//! Runs the hello-world bounded Axum web server (default: `127.0.0.1:3000`).

use std::net::SocketAddr;

use bounded_axum_server::hello_router;
use bounded_axum_server::server::{self, ServerLimits};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:3000".to_owned())
        .parse()?;

    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!(%local_addr, "listening");

    server::serve(listener, hello_router(), ServerLimits::default(), async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutting down");
    })
    .await;

    Ok(())
}
