use super::utils::*;
use std::io::Cursor;
use bytes::Bytes;
use calamine::{Data, Xlsx, open_workbook_from_rs, Reader};
use std::collections::HashSet;
use crate::error::AppError;
use crate::services::db_loader::DbLoader;
use polars::prelude::DataFrame;
use polars::prelude::*;
use polars::series::Series;
use polars::datatypes::{DataType, TimeUnit};

pub struct ExcelProcessor {
    db_loader: DbLoader,
}

impl ExcelProcessor {
    pub fn new(db_loader: DbLoader) -> Self {
        Self { db_loader }
    }

    pub async fn process_file(&self, file_data: Bytes) -> Result<u32, AppError> {
        tracing::info!("Processing Excel file");
        let cursor = Cursor::new(file_data);
        
        let mut workbook: Xlsx<_> = open_workbook_from_rs(cursor)
            .map_err(|e| AppError::FileProcessingError(format!("Failed to open Excel file: {}", e)))?;
    
        let mut total_tabs = 0;
        let sheet_names = workbook.sheet_names().to_vec();
        tracing::info!("Processing {} sheets", sheet_names.len());
    
        for sheet_name in &sheet_names {
            tracing::info!("Processing sheet: {}", sheet_name);
            match workbook.worksheet_range(sheet_name) {
                Ok(range) => {
                    let rows: Vec<Vec<Data>> = range.rows().map(|row| row.to_vec()).collect();
                    
                    if rows.is_empty() {
                        tracing::warn!("Sheet {} is empty, skipping", sheet_name);
                        continue;
                    }
    
                    let mut existing_names = HashSet::new();
                    let headers = rows.first()
                        .map(|row| row.iter()
                            .map(|cell| clean_column_name(&cell.to_string(), &mut existing_names))
                            .collect::<Vec<_>>())
                        .unwrap_or_default();
    
                    tracing::info!("Creating dataframe for sheet {} with {} rows", sheet_name, rows.len());
                    match self.create_dataframe(&rows, &headers) {
                        Ok(mut df) => {
                            if let Some(cleaned_df) = self.clean_dataframe(&df) {
                                df = cleaned_df;
                                
                                // Detect and normalize date columns
                                let date_columns = self.detect_date_columns(&df);
                                df = self.normalize_date_columns(&mut df, &date_columns);
    
                                // Generate a unique table name
                                let table_name = format!("excel_{}_{}", clean_table_name(sheet_name), chrono::Utc::now().timestamp());
                                tracing::info!("Loading sheet {} into table {}", sheet_name, table_name);
                                
                                // Load the data into SQLite
                                match self.db_loader.load_dataframe(df, &table_name).await {
                                    Ok(()) => {
                                        total_tabs += 1;
                                        tracing::info!("Successfully loaded sheet {} into database", sheet_name);
                                    }
                                    Err(e) => {
                                        tracing::error!("Failed to load sheet {} into database: {}", sheet_name, e);
                                    }
                                }
                            } else {
                                tracing::warn!("Sheet {} produced empty dataframe after cleaning", sheet_name);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to create dataframe for sheet {}: {}", sheet_name, e);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to read worksheet {}: {}", sheet_name, e);
                    continue;
                }
            }
        }
    
        if total_tabs == 0 {
            tracing::error!("No valid data found in Excel file after processing all sheets");
            Err(AppError::FileProcessingError("No valid data found in Excel file".to_string()))
        } else {
            tracing::info!("Successfully processed {} sheets", total_tabs);
            Ok(total_tabs)
        }
    }

    fn clean_dataframe(&self, df: &DataFrame) -> Option<DataFrame> {
        if df.height() == 0 || df.width() == 0 {
            return None;
        }
    
        // Drop rows where all values are null or empty
        let df = df.drop_nulls::<String>(None)
            .unwrap_or_else(|_| df.clone());
    
        // Drop columns where all values are null
        let df = df.select(
            df.get_columns()
                .iter()
                .filter(|series| !series.is_empty() && !series.is_null().all())
                .map(|series| series.name())
                .collect::<Vec<_>>()
        ).unwrap_or_else(|_| df.clone());
    
        if df.height() == 0 || df.width() == 0 {
            None
        } else {
            Some(df)
        }
    }

    fn create_dataframe(&self, rows: &[Vec<Data>], headers: &[String]) -> Result<DataFrame, AppError> {
        if rows.is_empty() || headers.is_empty() {
            return Err(AppError::InvalidInput("Empty data or headers".to_string()));
        }
    
        let mut columns = Vec::new();
        
        for (col_idx, header) in headers.iter().enumerate() {
            let values: Vec<Data> = rows.iter()
                .skip(1) // Skip header row
                .map(|row| row.get(col_idx).cloned().unwrap_or(Data::Empty))
                .collect();
            
            let series = match detect_column_type(&values) {
                t if t == "numeric" => {
                    let nums: Vec<Option<f64>> = values.iter().map(|v| match v {
                        Data::Float(f) => Some(*f),
                        Data::Int(i) => Some(*i as f64),
                        _ => None,
                    }).collect();
                    Series::new(header, nums)
                },
                t if t == "date" => {
                    let dates: Vec<Option<i64>> = values.iter().map(|v| match v {
                        Data::DateTime(d) => {
                            let days_since_1900 = d.as_f64();
                            let seconds = (days_since_1900 * 86400.0) as i64;
                            Some(seconds)
                        },
                        _ => None,
                    }).collect();
                    Series::new(header, dates)
                },
                _ => {
                    let strings: Vec<String> = values.iter().map(|v| v.to_string()).collect();
                    Series::new(header, strings)
                }
            };
            
            columns.push(series);
        }
        
        DataFrame::new(columns)
            .map_err(|e| AppError::InvalidInput(format!("Failed to create DataFrame: {}", e)))
    }

    fn detect_date_columns(&self, df: &DataFrame) -> Vec<String> {
        df.get_columns()
            .iter()
            .filter_map(|series| {
                let name = series.name();
                // Convert to string series and check first value
                series
                    .cast(&DataType::String)
                    .ok()  // Convert Result to Option
                    .and_then(|str_series| {
                        str_series
                            .str()
                            .ok()  // Convert Result to Option
                            .and_then(|ca| ca.get(0))  // This returns Option<&str>
                            .and_then(|val| {
                                if is_date_string(val) {
                                    Some(name.to_string())
                                } else {
                                    None
                                }
                            })
                    })
            })
            .collect()
    }

    fn normalize_date_columns(&self, df: &mut DataFrame, date_columns: &[String]) -> DataFrame {
        for col_name in date_columns {
            if let Ok(series) = df.column(col_name) {
                if let Ok(dates) = series.cast(&DataType::Datetime(TimeUnit::Microseconds, None)) {
                    let _ = df.replace(col_name, dates);
                }
            }
        }
        df.clone()
    }
}