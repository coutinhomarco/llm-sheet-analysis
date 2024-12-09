use super::types::*;
use super::utils::*;
use std::io::Cursor;
use bytes::Bytes;
use calamine::{Data, Xlsx, open_workbook_from_rs};
use std::collections::HashSet;
use smallvec::SmallVec;
use crate::error::AppError;
use rayon::prelude::*;
use calamine::Reader;
use std::sync::{Arc, Mutex};
use super::types::SAMPLE_SIZE;
const TYPE_DETECTION_ROWS: usize = 100;
pub struct ExcelAnalyzer;

impl ExcelAnalyzer {
    pub async fn analyze_from_bytes(&self, file_data: Bytes) -> Result<SheetAnalysis, AppError> {
        let start = std::time::Instant::now();
        tracing::info!("Starting Excel file analysis from bytes");
        
        // Create a memory-mapped file for better performance with large files
        let cursor = Cursor::new(file_data);
        
        tracing::info!("Opening workbook...");
        let workbook_start = std::time::Instant::now();
        let mut workbook: Xlsx<_> = open_workbook_from_rs(cursor)
            .map_err(|e| {
                tracing::error!("Failed to open Excel file: {}", e);
                AppError::FileProcessingError(format!("Failed to open Excel file: {}", e))
            })?;
        tracing::info!("Workbook opened in {:?}", workbook_start.elapsed());
        
        let sheet_names: Vec<String> = workbook.sheet_names().to_vec();
        tracing::info!("Found {} sheets: {:?}", sheet_names.len(), sheet_names);
        
        if let Some(sheet_name) = sheet_names.first() {
            let worksheets = workbook.worksheets();
            if let Some((_, range)) = worksheets.into_iter().find(|(name, _)| name == sheet_name) {
                // Use a streaming iterator for rows to reduce memory usage
                let mut rows = Vec::with_capacity(1000);
                let mut row_iter = range.rows();
                
                // Process header row separately
                if let Some(header_row) = row_iter.next() {
                    rows.push(header_row.to_vec());
                    
                    // Process remaining rows in chunks
                    for row in row_iter.take(999) {
                        rows.push(row.to_vec());
                    }
                }
    
                let row_count = rows.len();
                let column_count = rows.first().map_or(0, |r| r.len());
                
                // Process headers with thread-safe name tracking
                let mut existing_names = HashSet::new();
                let headers = rows.first()
                    .map(|row| {
                        row.iter()
                            .map(|cell| clean_column_name(&cell.to_string(), &mut existing_names))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
            
                    let date_columns = Arc::new(Mutex::new(Vec::new()));
                    let numeric_columns = Arc::new(Mutex::new(Vec::new()));
                    let text_columns = Arc::new(Mutex::new(Vec::new()));
            
    
                let column_info: Vec<ColumnInfo> = headers.par_iter()
                .enumerate()
                .map(|(idx, name)| {
                    let values: Vec<Data> = rows.iter()
                        .skip(1)
                        .take(TYPE_DETECTION_ROWS)
                        .filter_map(|row| row.get(idx))
                        .cloned()
                        .collect();
                    
                    let data_type = self.detect_column_type(&values);
                    
                    // Use thread-safe operations for column categorization
                    match data_type.as_str() {
                        "date" => {
                            if let Ok(mut cols) = date_columns.lock() {
                                cols.push(name.clone());
                            }
                        },
                        "numeric" => {
                            if let Ok(mut cols) = numeric_columns.lock() {
                                cols.push(name.clone());
                            }
                        },
                        "string" => {
                            if let Ok(mut cols) = text_columns.lock() {
                                cols.push(name.clone());
                            }
                        },
                        _ => {}
                    }
                    
                    self.analyze_column(&values, name)
                })
                .collect();
            
            // Before creating SheetAnalysis, unwrap the mutex values
            let date_columns = Arc::try_unwrap(date_columns)
                .unwrap_or_else(|_| panic!("Failed to unwrap date_columns"))
                .into_inner()
                .unwrap_or_default();
            
            let numeric_columns = Arc::try_unwrap(numeric_columns)
                .unwrap_or_else(|_| panic!("Failed to unwrap numeric_columns"))
                .into_inner()
                .unwrap_or_default();
            
            let text_columns = Arc::try_unwrap(text_columns)
                .unwrap_or_else(|_| panic!("Failed to unwrap text_columns"))
                .into_inner()
                .unwrap_or_default();
    
                tracing::info!("Analysis completed in {:?}", start.elapsed());
                

                let sample_data: Vec<Vec<String>> = rows.iter()
                    .take(SAMPLE_SIZE)
                    .map(|row| {
                        row.iter()
                            .map(|cell| cell.to_string())
                            .collect()
                    })
                    .collect();

                Ok(SheetAnalysis {
                    sheet_names,
                    row_count,
                    column_count,
                    sample_data,
                    column_info,
                    dataframe: None,
                    date_columns,
                    numeric_columns,
                    text_columns,
                })
            } else {
                Err(AppError::FileProcessingError("Failed to read worksheet".to_string()))
            }
        } else {
            Err(AppError::FileProcessingError("No sheets found in workbook".to_string()))
        }
    }
    fn analyze_column(&self, values: &[Data], name: &str) -> ColumnInfo {
        let mut sample_values = SmallVec::<[String; SAMPLE_SIZE]>::new();
    
    let (null_count, seen_values, min_max) = values.par_iter()
        .fold(
            || (0, HashSet::new(), (None, None)),
            |(mut nulls, mut seen, mut min_max), value| {
                let str_value = value.to_string();
                if matches!(value, Data::Empty) {
                    nulls += 1;
                } else {
                    seen.insert(str_value.clone());
                    update_min_max(&mut min_max, &str_value);
                }
                (nulls, seen, min_max)
            }
        )
        .reduce(
            || (0, HashSet::new(), (None, None)),
            |a, b| {
                let mut combined_set = a.1;
                combined_set.extend(b.1);
                (
                    a.0 + b.0,
                    combined_set,
                    merge_min_max(a.2, b.2)
                )
            }
        );

    // Get sample values
    values.iter()
        .take(SAMPLE_SIZE)
        .for_each(|value| {
            sample_values.push(match value {
                Data::Empty => "".to_string(),
                _ => value.to_string()
            });
        });

    ColumnInfo {
        name: name.to_string(),
        data_type: self.detect_column_type(values),
        sample_values,
        null_count,
        unique_count: seen_values.len(),
        min_value: min_max.0,
        max_value: min_max.1,
        has_duplicates: seen_values.len() < values.len() - null_count,
    }
    }

    fn detect_column_type(&self, values: &[Data]) -> String {
        let (numeric_count, date_count, bool_count, empty_count) = values.par_iter()
            .take(TYPE_DETECTION_ROWS)
            .filter(|v| !matches!(v, Data::Empty))
            .fold(
                || (0, 0, 0, 0),
                |(mut num, mut date, mut bool, mut empty), value| {
                    match value {
                        Data::Float(_) | Data::Int(_) => num += 1,
                        Data::DateTime(_) => date += 1,
                        Data::String(s) if is_date_string(s) => date += 1,
                        Data::Bool(_) => bool += 1,
                        Data::Empty => empty += 1,
                        _ => {}
                    }
                    (num, date, bool, empty)
                }
            )
            .reduce(|| (0, 0, 0, 0),
                |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2, a.3 + b.3)
            );

    let total = values.len() - empty_count;
    if total == 0 {
        return "empty".to_string();
    }

    let threshold = total as f64 * 0.8;
    match () {
        _ if numeric_count as f64 >= threshold => "numeric",
        _ if date_count as f64 >= threshold => "date",
        _ if bool_count as f64 >= threshold => "boolean",
        _ => "string",
    }.to_string()
    }
}