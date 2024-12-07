use anyhow::Result;
use calamine::{Reader, Xlsx, open_workbook_from_rs, Data};
use polars::prelude::*;
use crate::error::AppError;
use regex::Regex;
use std::io::Cursor;
use bytes::Bytes;
use crate::services::db_loader::DbLoader;
use reqwest::Client;
use rayon::prelude::*;
use smallvec::SmallVec;
use std::collections::HashSet;

const SAMPLE_SIZE: usize = 3;
const TYPE_DETECTION_ROWS: usize = 50;
// const MAX_ANALYSIS_ROWS: usize = 1000;

#[derive(Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub sample_values: SmallVec<[String; SAMPLE_SIZE]>,
    pub null_count: usize,
    pub unique_count: usize,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub has_duplicates: bool,
}

#[derive(Debug)]
pub struct SheetAnalysis {
    pub sheet_names: Vec<String>,
    pub row_count: usize,
    pub column_count: usize,
    pub sample_data: Vec<Vec<String>>,
    pub column_info: Vec<ColumnInfo>,
    pub dataframe: Option<DataFrame>,
    pub date_columns: Vec<String>,
    pub numeric_columns: Vec<String>,
    pub text_columns: Vec<String>,
}

fn detect_column_type(values: &[Data]) -> String {
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
fn is_date_string(s: &str) -> bool {
    let patterns = [
        r"^\d{4}-\d{2}-\d{2}$",
        r"^\d{2}/\d{2}/\d{4}$",
        r"^\d{4}/\d{2}/\d{2}$",
        r"^\d{2}-\d{2}-\d{4}$",
    ];
    
    patterns.iter().any(|pattern| {
        Regex::new(pattern).map_or(false, |re| re.is_match(s))
    })
}

fn analyze_column(values: &[Data], name: &str) -> ColumnInfo {
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
        data_type: detect_column_type(values),
        sample_values,
        null_count,
        unique_count: seen_values.len(),
        min_value: min_max.0,
        max_value: min_max.1,
        has_duplicates: seen_values.len() < values.len() - null_count,
    }
}

fn clean_column_name(name: &str, existing_names: &mut HashSet<String>) -> String {
    let base_name = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect::<String>()
        .to_lowercase();
    
    let mut cleaned = if base_name.chars().next().map_or(true, |c| !c.is_alphabetic()) {
        format!("col_{}", base_name)
    } else {
        base_name
    };

    // If the name already exists, add a numeric suffix
    let mut counter = 1;
    let original_name = cleaned.clone();
    while !existing_names.insert(cleaned.clone()) {
        cleaned = format!("{}_{}", original_name, counter);
        counter += 1;
    }

    cleaned
}

fn create_dataframe(rows: &[Vec<Data>], headers: &[String]) -> Result<DataFrame, AppError> {
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
                        // Convert Excel datetime to Unix timestamp
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

pub async fn process_excel_file(file_data: Bytes, _file_extension: &str, db_loader: &DbLoader) -> Result<u32, AppError> {
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
                match create_dataframe(&rows, &headers) {
                    Ok(mut df) => {
                        if let Some(cleaned_df) = clean_dataframe(&df) {
                            df = cleaned_df;
                            
                            // Detect and normalize date columns
                            let date_columns = detect_date_columns(&df);
                            df = normalize_date_columns(&mut df, &date_columns);

                            // Generate a unique table name
                            let table_name = format!("excel_{}_{}", clean_table_name(sheet_name), chrono::Utc::now().timestamp());
                            tracing::info!("Loading sheet {} into table {}", sheet_name, table_name);
                            
                            // Load the data into SQLite
                            match db_loader.load_dataframe(df, &table_name).await {
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

fn clean_dataframe(df: &DataFrame) -> Option<DataFrame> {
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

fn normalize_date_columns(df: &mut DataFrame, date_columns: &[String]) -> DataFrame {
    for col_name in date_columns {
        if let Ok(series) = df.column(col_name) {
            if let Ok(dates) = series.cast(&DataType::Datetime(TimeUnit::Microseconds, None)) {
                let _ = df.replace(col_name, dates);
            }
        }
    }
    df.clone()
}

fn detect_date_columns(df: &DataFrame) -> Vec<String> {
    df.get_columns()
        .iter()
        .filter_map(|series| {
            let name = series.name();
            if is_date_series(series) {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn is_date_series(series: &Series) -> bool {
    match series.dtype() {
        DataType::Date => true,
        DataType::Datetime(_, _) => true,
        DataType::String => {
            // Sample first 100 non-null values and check if they match date patterns
            let sample = series
                .str()
                .expect("Already checked it's a string series")
                .into_iter()
                .filter_map(|opt_str| opt_str)
                .take(100);

            let mut date_count = 0;
            let mut total_count = 0;

            for value in sample {
                total_count += 1;
                if is_date_string(&value) {
                    date_count += 1;
                }
            }

            total_count > 0 && (date_count as f64 / total_count as f64) >= 0.8
        }
        _ => false,
    }
}

fn clean_table_name(name: &str) -> String {
    let cleaned = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect::<String>()
        .to_lowercase();
    
    if cleaned.chars().next().map_or(true, |c| !c.is_alphabetic()) {
        format!("tbl_{}", cleaned)
    } else {
        cleaned
    }
}

pub async fn load_file_from_url(url: &str) -> Result<Bytes, AppError> {
    let client = Client::new();
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| AppError::FileProcessingError(format!("Failed to fetch file: {}", e)))?;

    if !response.status().is_success() {
        return Err(AppError::FileProcessingError(
            format!("Failed to fetch file. Status: {}", response.status())
        ));
    }

    response
        .bytes()
        .await
        .map_err(|e| AppError::FileProcessingError(format!("Failed to read response bytes: {}", e)))
}

pub async fn analyze_excel_file_from_bytes(file_data: Bytes) -> Result<SheetAnalysis, AppError> {
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
                    
                    let data_type = detect_column_type(&values);
                    match data_type.as_str() {
                        "date" => date_columns.push(name.clone()),
                        "numeric" => numeric_columns.push(name.clone()),
                        "string" => text_columns.push(name.clone()),
                        _ => {}
                    }
                    
                    analyze_column(&values, name)
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

fn update_min_max(min_max: &mut (Option<String>, Option<String>), value: &str) {
    match &min_max.0 {
        None => min_max.0 = Some(value.to_string()),
        Some(min) if value.to_string() < *min => min_max.0 = Some(value.to_string()),
        _ => {}
    }
    match &min_max.1 {
        None => min_max.1 = Some(value.to_string()),
        Some(max) if value.to_string() > *max => min_max.1 = Some(value.to_string()),
        _ => {}
    }
}

fn merge_min_max(
    a: (Option<String>, Option<String>), 
    b: (Option<String>, Option<String>)
) -> (Option<String>, Option<String>) {
    let min = match (a.0, b.0) {
        (None, None) => None,
        (Some(v), None) | (None, Some(v)) => Some(v),
        (Some(v1), Some(v2)) => Some(if v1 < v2 { v1 } else { v2 }),
    };
    let max = match (a.1, b.1) {
        (None, None) => None,
        (Some(v), None) | (None, Some(v)) => Some(v),
        (Some(v1), Some(v2)) => Some(if v1 > v2 { v1 } else { v2 }),
    };
    (min, max)
}