use axum::{
    response::{IntoResponse, Response},
    http::StatusCode,
};
use serde_json::json;
use axum::Json;

#[derive(Debug)]
pub enum AppError {
    InvalidInput(String),
    IoError(std::io::Error),
    LlmError(String),
    ParseError(String),
    DatabaseError(String),
    Internal(String),
    OpenAI(String),
    AwsS3(String),
    Database(String),
    HttpError(String),
    FileProcessingError(String),
    DataFrameError(String),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::HttpError(msg) => write!(f, "HTTP Error: {}", msg),
            AppError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            AppError::IoError(err) => write!(f, "IO error: {}", err),
            AppError::LlmError(msg) => write!(f, "LLM error: {}", msg),
            AppError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            AppError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            AppError::Internal(msg) => write!(f, "Internal error: {}", msg),
            AppError::OpenAI(msg) => write!(f, "OpenAI error: {}", msg),
            AppError::AwsS3(msg) => write!(f, "AWS S3 error: {}", msg),
            AppError::Database(msg) => write!(f, "Database error: {}", msg),
            AppError::FileProcessingError(msg) => write!(f, "File processing error: {}", msg),
            AppError::DataFrameError(msg) => write!(f, "DataFrame error: {}", msg),
        }
    }
}

impl std::error::Error for AppError {}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        AppError::IoError(err)
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(err: rusqlite::Error) -> Self {
        AppError::DatabaseError(err.to_string())
    }
}

impl From<serde_json::Error> for AppError {
    fn from(err: serde_json::Error) -> Self {
        AppError::ParseError(err.to_string())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::HttpError(msg) =>(StatusCode::BAD_REQUEST, msg),
            AppError::InvalidInput(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::IoError(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            AppError::LlmError(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg),
            AppError::ParseError(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::DatabaseError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            AppError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            AppError::OpenAI(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg),
            AppError::AwsS3(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg),
            AppError::Database(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            AppError::FileProcessingError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            AppError::DataFrameError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = Json(json!({
            "error": message
        }));

        (status, body).into_response()
    }
}