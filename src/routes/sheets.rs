use axum::{
    extract::State,
    routing::post,
    Router,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::{AppState, error::AppError, services::file_processor};

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/sheets/analyze", post(analyze_sheet))
}

#[derive(Debug, Deserialize)]
pub struct AnalyzeRequest {
    file_path: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct ColumnAnalysis {
    name: String,
    data_type: String,
    sample_values: Vec<String>,
    null_count: usize,
    unique_count: usize,
    min_value: Option<String>,
    max_value: Option<String>,
    has_duplicates: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct AnalyzeResponse {
    sheet_names: Vec<String>,
    row_count: usize,
    column_count: usize,
    sample_data: Vec<Vec<String>>,
    column_analysis: Vec<ColumnAnalysis>,
    date_columns: Vec<String>,
    numeric_columns: Vec<String>,
    text_columns: Vec<String>,
}

async fn analyze_sheet(
    State(_state): State<Arc<AppState>>,
    Json(request): Json<AnalyzeRequest>,
) -> Result<Json<AnalyzeResponse>, AppError> {
    tracing::info!("Analyzing sheet from path: {}", request.file_path);
    
    let analysis = file_processor::analyze_excel_file(&request.file_path).await?;
    
    let column_analysis = analysis.column_info.into_iter()
        .map(|info| ColumnAnalysis {
            name: info.name,
            data_type: info.data_type,
            sample_values: info.sample_values,
            null_count: info.null_count,
            unique_count: info.unique_count,
            min_value: info.min_value,
            max_value: info.max_value,
            has_duplicates: info.has_duplicates,
        })
        .collect();

    Ok(Json(AnalyzeResponse {
        sheet_names: analysis.sheet_names,
        row_count: analysis.row_count,
        column_count: analysis.column_count,
        sample_data: analysis.sample_data,
        column_analysis,
        date_columns: analysis.date_columns,
        numeric_columns: analysis.numeric_columns,
        text_columns: analysis.text_columns,
    }))
}