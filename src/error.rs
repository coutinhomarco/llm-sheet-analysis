use axum::{
    response::{IntoResponse, Response},
    http::StatusCode,
};
use serde_json::json;

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("Internal server error")]
    Internal(#[from] anyhow::Error),
    
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    
    #[error("Database error: {0}")]
    Database(String),
    
    #[error("OpenAI API error: {0}")]
    OpenAI(String),
    
    #[error("AWS S3 error: {0}")]
    AwsS3(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error"),
            AppError::InvalidInput(msg) => (StatusCode::BAD_REQUEST, msg.as_str()),
            AppError::Database(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Database error"),
            AppError::OpenAI(_) => (StatusCode::SERVICE_UNAVAILABLE, "LLM service error"),
            AppError::AwsS3(_) => (StatusCode::SERVICE_UNAVAILABLE, "Storage service error"),
        };

        let body = json!({
            "error": message,
        });

        (status, axum::Json(body)).into_response()
    }
}