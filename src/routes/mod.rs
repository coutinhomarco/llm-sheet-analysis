use axum::{routing::get, Router, http::Method};
use tower_http::cors::{CorsLayer, Any};
use std::sync::Arc;
use crate::AppState;

pub mod sheets;

pub fn routes() -> Router<Arc<AppState>> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
        .max_age(std::time::Duration::from_secs(3600));

    Router::new()
        .route("/health", get(health_check))
        .merge(sheets::routes())
        .layer(cors)
}

async fn health_check() -> &'static str {
    "OK"
}