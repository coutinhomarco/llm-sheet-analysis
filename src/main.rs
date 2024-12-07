use anyhow::Result;
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use crate::config::Config;
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
    let config = Config::new()?;
    
    // Create app state
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
    pub config: Config,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}
