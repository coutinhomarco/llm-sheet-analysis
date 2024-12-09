use bytes::Bytes;
use crate::error::AppError;
use crate::services::{
    excel::{ExcelAnalyzer, ExcelProcessor, types::*},
    db_loader::DbLoader,
};
use std::sync::Arc;
use std::time::Duration;
use reqwest::Client;
use tokio::time::sleep;
use tracing::{info, warn};
use tokio::sync::OnceCell;
use lru::LruCache;
use std::sync::Mutex;
use std::num::NonZeroUsize;

// Constants for configuration
const MAX_RETRIES: u32 = 3;
const CACHE_MAX_CAPACITY: usize = 100;
const REQUEST_TIMEOUT_SECS: u64 = 30;

pub struct FileProcessor {
    client: Client,
    file_cache: Arc<Mutex<LruCache<String, Bytes>>>,
}

impl FileProcessor {
    pub fn new() -> Result<Self, AppError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .map_err(|e| AppError::FileProcessingError(format!("Failed to create HTTP client: {}", e)))?;
    
        let cache_capacity = NonZeroUsize::new(CACHE_MAX_CAPACITY)
            .ok_or_else(|| AppError::FileProcessingError("Invalid cache capacity".to_string()))?;
        let file_cache = Arc::new(Mutex::new(LruCache::new(cache_capacity)));
    
        Ok(Self {
            client,
            file_cache,
        })
    }

    pub async fn load_file_from_url(&self, url: &str) -> Result<Bytes, AppError> {
        // Check cache first
        if let Some(cached_data) = self.file_cache.lock()
            .map_err(|e| AppError::FileProcessingError(format!("Cache lock error: {}", e)))?
            .get(url) {
            info!("File found in cache: {}", url);
            return Ok(cached_data.clone());
        }

        let mut retries = 0;
        let mut last_error = None;

        while retries < MAX_RETRIES {
            match self.attempt_file_download(url).await {
                Ok(file_data) => {
                    // Cache the successful result
                    if let Ok(mut cache) = self.file_cache.lock() {
                        cache.put(url.to_string(), file_data.clone());
                    }
                    return Ok(file_data);
                }
                Err(e) => {
                    warn!("Attempt {} failed to download file {}: {}", retries + 1, url, e);
                    last_error = Some(e);
                    retries += 1;

                    if retries < MAX_RETRIES {
                        let delay = Duration::from_secs(2u64.pow(retries));
                        sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            AppError::FileProcessingError("Maximum retries exceeded".to_string())
        }))
    }

    async fn attempt_file_download(&self, url: &str) -> Result<Bytes, AppError> {
        info!("Downloading file from URL: {}", url);
        
        let response = self.client
            .get(url)
            .send()
            .await
            .map_err(|e| AppError::FileProcessingError(format!("Failed to download file: {}", e)))?;

        if !response.status().is_success() {
            return Err(AppError::FileProcessingError(
                format!("Failed to download file. Status: {}", response.status())
            ));
        }

        response
            .bytes()
            .await
            .map_err(|e| AppError::FileProcessingError(format!("Failed to read file bytes: {}", e)))
    }
}

// Singleton instance for the FileProcessor
static FILE_PROCESSOR: OnceCell<FileProcessor> = OnceCell::const_new();

// Public interface functions
pub async fn analyze_excel_file_from_bytes(file_data: Bytes) -> Result<SheetAnalysis, AppError> {
    info!("Starting Excel file analysis");
    let analyzer = ExcelAnalyzer;
    analyzer.analyze_from_bytes(file_data).await
}

pub async fn process_excel_file(file_data: Bytes, db_loader: &DbLoader) -> Result<u32, AppError> {
    info!("Starting Excel file processing");
    let processor = ExcelProcessor::new(db_loader.clone());
    processor.process_file(file_data).await
}

pub async fn load_file_from_url(url: &str) -> Result<Bytes, AppError> {
    let processor = FILE_PROCESSOR
        .get_or_try_init(|| async { FileProcessor::new() })
        .await
        .map_err(|e| AppError::FileProcessingError(format!("Failed to initialize FileProcessor: {}", e)))?;
    
    processor.load_file_from_url(url).await
}