use codex_openai_gateway::{app, Config, GatewayState};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();
    let config = Config::from_env()?;
    let addr = config.listen_addr()?;
    let state = GatewayState::start(config).await?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "codex gateway listening");
    axum::serve(listener, app(state)).with_graceful_shutdown(shutdown()).await?;
    Ok(())
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}
