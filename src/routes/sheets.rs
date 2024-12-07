use axum::{
    extract::State,
    routing::post,
    Router,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::{
    AppState, 
    error::AppError, 
    services::{
        file_processor,
        db_loader::DbLoader,
        llm_agent::{LlmAgent, AgentResponse}
    }
};

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/sheets/analyze", post(analyze_sheet))
}

#[derive(Debug, Deserialize)]
pub struct AnalyzeRequest {
    file_path: String,
    should_generate_new_file: Option<bool>,
    user_email: Option<String>,
    chat_id: Option<String>,
    messages: Option<Vec<String>>,
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
    tool_result: AgentResponse,
    new_file_url: Option<String>,
}

#[axum::debug_handler]
async fn analyze_sheet(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AnalyzeRequest>,
) -> Result<Json<FullAnalysisResponse>, AppError> {
    tracing::info!("Analyzing sheet from path: {}", request.file_path);
    let start = std::time::Instant::now();

    // 1. Analyze file structure and get DataFrame
    let analysis = file_processor::analyze_excel_file(&request.file_path).await?;
    
    // 2. Create DbLoader and load data into SQLite
    let db_loader = DbLoader::new()?;
    let table_name = format!("excel_{}", chrono::Utc::now().timestamp());
    
    // Only try to load if we have a DataFrame
    if let Some(df) = analysis.dataframe.clone() {
        tracing::info!("Loading DataFrame into database");
        db_loader.load_dataframe(df, &table_name).await?;
        tracing::info!("Successfully loaded DataFrame");
    } else {
        tracing::warn!("No DataFrame available to load");
    }
    
    let llm_agent = LlmAgent::new_with_loader(&state.config.openai_key, db_loader)?;
    let messages = request.messages.unwrap_or_default();
    let agent_response = llm_agent.generate_analysis(&messages).await?;
    tracing::info!("Analysis completed SHEET.RS 91 in {:?}", start.elapsed());

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
                    sample_values: info.sample_values,
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
        tool_result: agent_response,
        new_file_url: None,
    }))
}