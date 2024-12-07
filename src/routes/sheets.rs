use axum::{
    extract::State,
    routing::post,
    Router,
    Json,
    http::Method,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::{
    AppState, 
    error::AppError, 
    services::{
        file_processor,
        db_loader::DbLoader,
        llm_agent::{LlmAgent, QueryResult}
    }
};
use tower_http::cors::{CorsLayer, Any};

pub fn routes() -> Router<Arc<AppState>> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
        .max_age(std::time::Duration::from_secs(3600));

    Router::new()
        .route("/sheets/analyze", post(analyze_sheet))
        .layer(cors)
}

#[derive(Debug, Deserialize)]
pub struct FileInfo {
    #[serde(rename = "type")]
    file_type: String,
    signed_url: String,
}

#[derive(Debug, Deserialize)]
pub struct AnalyzeRequest {
    user_email: String,
    chat_id: String,
    messages: Vec<String>,
    files: Vec<FileInfo>,
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

#[derive(Debug, Serialize)]
pub struct FullAnalysisResponse {
    analysis: AnalyzeResponse,
    tool_result: QueryResult,
    new_file_url: Option<String>,
}

#[axum::debug_handler]
async fn analyze_sheet(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AnalyzeRequest>,
) -> Result<Json<FullAnalysisResponse>, AppError> {
    let start = std::time::Instant::now();
    tracing::info!(
        "Starting analysis for user: {}, chat_id: {}", 
        request.user_email, 
        request.chat_id
    );

    // 1. Validate file type and get URL
    let file_info = request.files.first()
        .ok_or_else(|| AppError::InvalidInput("No file provided".to_string()))?;
    
    tracing::info!(
        "Processing file type: {}, URL length: {}", 
        file_info.file_type,
        file_info.signed_url.len()
    );

    if !file_info.file_type.to_lowercase().contains("xlsx") {
        tracing::error!("Unsupported file type: {}", file_info.file_type);
        return Err(AppError::InvalidInput("Only XLSX files are supported".to_string()));
    }

    // 2. Download file from URL (only once)
    tracing::info!("Downloading file from URL...");
    let download_start = std::time::Instant::now();
    let file_data = file_processor::load_file_from_url(&file_info.signed_url).await?;
    tracing::info!("File downloaded, size: {}KB, took: {:?}", file_data.len() / 1024, download_start.elapsed());
    
    // 3. Create DbLoader
    tracing::info!("Initializing database loader...");
    let db_start = std::time::Instant::now();
    let db_loader = DbLoader::new().await?;
    tracing::info!("Database loader initialized in {:?}", db_start.elapsed());
    
    // 4. Analyze Excel file structure using the downloaded data
    tracing::info!("Starting Excel file analysis...");
    let analysis_start = std::time::Instant::now();
    let analysis = file_processor::analyze_excel_file_from_bytes(file_data.clone()).await?;
    tracing::info!(
        "Excel analysis completed in {:?}. Found {} sheets, {} rows, {} columns",
        analysis_start.elapsed(),
        analysis.sheet_names.len(),
        analysis.row_count,
        analysis.column_count
    );
    
    // 5. Process Excel file and load into database
    tracing::info!("Loading data into database...");
    let db_load_start = std::time::Instant::now();
    let tables_created = file_processor::process_excel_file(file_data, &db_loader).await?;
    tracing::info!("Created {} tables in database in {:?}", tables_created, db_load_start.elapsed());
    
    // 6. Generate LLM analysis
    tracing::info!("Starting LLM analysis...");
    let llm_start = std::time::Instant::now();
    let llm_agent = LlmAgent::new_with_loader(&state.config.openai_key, db_loader)?;
    let agent_response = llm_agent.generate_analysis(&request.messages).await?;
    let query_result = llm_agent.execute_queries(agent_response).await?;
    tracing::info!("LLM analysis completed in {:?}", llm_start.elapsed());
    
    tracing::info!("Total processing completed in {:?}", start.elapsed());

    Ok(Json(FullAnalysisResponse {
        analysis: AnalyzeResponse {
            sheet_names: analysis.sheet_names,
            row_count: analysis.row_count,
            column_count: analysis.column_count,
            sample_data: analysis.sample_data,
            column_analysis: analysis.column_info.into_iter()
                .map(|info| ColumnAnalysis {
                    name: info.name,
                    data_type: info.data_type,
                    sample_values: info.sample_values.to_vec(),
                    null_count: info.null_count,
                    unique_count: info.unique_count,
                    min_value: info.min_value,
                    max_value: info.max_value,
                    has_duplicates: info.has_duplicates,
                })
                .collect(),
            date_columns: analysis.date_columns,
            numeric_columns: analysis.numeric_columns,
            text_columns: analysis.text_columns,
        },
        tool_result: query_result,
        new_file_url: None,
    }))
}