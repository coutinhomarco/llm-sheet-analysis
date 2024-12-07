use anyhow::Result;
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;

mod config;
mod error;
mod logging;
mod routes;
mod services;
pub mod models;
pub mod clients;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    logging::init_logging()?;
    
    // Load configuration
    let config = config::load_config()?;
    
    // Build our application state
    let state = Arc::new(AppState::new(config));
    
    // Build our application with a route
    let app = Router::new()
        .merge(routes::sheets::routes())
        .with_state(state);

    // Run it
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// Application state
#[derive(Clone)]
pub struct AppState {
    config: config::Config,
}

impl AppState {
    fn new(config: config::Config) -> Self {
        Self { config }
    }
}
