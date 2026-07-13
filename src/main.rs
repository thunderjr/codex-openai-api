use codex_openai_gateway::{app, Config, GatewayState};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let config = Config::from_env()?;
    let addr = config.listen_addr()?;
    let state = GatewayState::start(config).await?;
    let backend = state.backend.clone();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "codex gateway listening");
    axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown())
        .await?;
    // Reap the pool's app-server children (and the code-mode hosts under them)
    // so a SIGTERM/Ctrl-C never leaves orphaned Codex processes behind.
    backend.shutdown().await;
    Ok(())
}

async fn shutdown() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
