use bytes::Bytes;
use crate::error::AppError;
use crate::services::{
    excel::{ExcelAnalyzer, ExcelProcessor, types::*},
    db_loader::DbLoader,
};

pub async fn analyze_excel_file_from_bytes(file_data: Bytes) -> Result<SheetAnalysis, AppError> {
    let analyzer = ExcelAnalyzer;
    analyzer.analyze_from_bytes(file_data).await
}

pub async fn process_excel_file(file_data: Bytes, db_loader: &DbLoader) -> Result<u32, AppError> {
    let processor = ExcelProcessor::new(db_loader.clone());
        
    processor.process_file(file_data).await
}

pub async fn load_file_from_url(url: &str) -> Result<Bytes, AppError> {
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| AppError::FileProcessingError(format!("Failed to download file: {}", e)))?;
    
    response
        .bytes()
        .await
        .map_err(|e| AppError::FileProcessingError(format!("Failed to read file bytes: {}", e)))
}