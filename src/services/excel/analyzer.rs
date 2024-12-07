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
use super::types::SAMPLE_SIZE;
const TYPE_DETECTION_ROWS: usize = 100;
pub struct ExcelAnalyzer;

impl ExcelAnalyzer {
    pub async fn analyze_from_bytes(&self, file_data: Bytes) -> Result<SheetAnalysis, AppError> {
       
    let start = std::time::Instant::now();
    tracing::info!("Starting Excel file analysis from bytes");
    
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
            // Get only first 1000 rows for analysis
            let rows: Vec<Vec<Data>> = range.rows()
                .take(1000)
                .map(|row| row.to_vec())
                .collect();

            let row_count = rows.len();
            let column_count = rows.first().map_or(0, |r| r.len());
            
            let mut existing_names = HashSet::new();
            let headers = rows.first()
                .map(|row| row.iter()
                    .map(|cell| clean_column_name(&cell.to_string(), &mut existing_names))
                    .collect::<Vec<_>>())
                .unwrap_or_default();

            // Quick column type detection
            let mut date_columns: Vec<String> = Vec::new();
            let mut numeric_columns: Vec<String> = Vec::new();
            let mut text_columns: Vec<String> = Vec::new();
            
            // Take sample rows for analysis
            let sample_data: Vec<Vec<String>> = rows.iter()
                .take(5)
                .map(|row| row.iter().map(|cell| cell.to_string()).collect())
                .collect();

            // Basic column analysis
            let column_info = headers.iter().enumerate()
                .map(|(idx, name)| {
                    let values: Vec<Data> = rows.iter()
                        .skip(1)
                        .take(100) // Only analyze first 100 rows for type detection
                        .map(|row| row.get(idx).cloned().unwrap_or(Data::Empty))
                        .collect();
                    
                        let data_type = self.detect_column_type(&values);
                    match data_type.as_str() {
                        "date" => date_columns.push(name.clone()),
                        "numeric" => numeric_columns.push(name.clone()),
                        "string" => text_columns.push(name.clone()),
                        _ => {}
                    }
                    
                    self.analyze_column(&values, name)
                })
                .collect();

            tracing::info!("Analysis completed FILEPROCESSOR 575 in {:?}", start.elapsed());
            
            Ok(SheetAnalysis {
                sheet_names: sheet_names,
                row_count,
                column_count,
                sample_data: sample_data,
                column_info: column_info,
                dataframe: None,
                date_columns: date_columns,
                numeric_columns: numeric_columns,
                text_columns: text_columns,
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