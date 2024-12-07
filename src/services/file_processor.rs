use anyhow::Result;
use calamine::{Reader, Xlsx, open_workbook, open_workbook_from_rs, Data};
use polars::prelude::*;
use std::path::Path;
use crate::error::AppError;
use regex::Regex;
use std::io::Cursor;
use polars::io::csv::CsvReader;
use bytes::Bytes;
use crate::services::db_loader::DbLoader;
use reqwest::Client;

#[derive(Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub sample_values: Vec<String>,
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
    let mut numeric_count = 0;
    let mut date_count = 0;
    let mut _string_count = 0;
    let mut bool_count = 0;
    let mut empty_count = 0;
    
    for value in values {
        match value {
            Data::Float(_) | Data::Int(_) => numeric_count += 1,
            Data::DateTime(_) => date_count += 1,
            Data::String(s) => {
                if is_date_string(s) {
                    date_count += 1;
                } else {
                    _string_count += 1;
                }
            },
            Data::Bool(_) => bool_count += 1,
            Data::Empty => empty_count += 1,
            _ => {}
        }
    }
    
    let total = values.len() - empty_count;
    if total == 0 {
        return "empty".to_string();
    }
    
    let threshold = total as f64 * 0.8; // 80% threshold
    
    if numeric_count as f64 >= threshold { "numeric" }
    else if date_count as f64 >= threshold { "date" }
    else if bool_count as f64 >= threshold { "boolean" }
    else { "string" }.to_string()
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
    let mut sample_values = Vec::new();
    let mut null_count = 0;
    let mut seen_values = std::collections::HashSet::new();
    let mut min_value: Option<String> = None;
    let mut max_value: Option<String> = None;

    for value in values.iter().take(5) {
        let str_value = match value {
            Data::Empty => {
                null_count += 1;
                "".to_string()
            },
            _ => value.to_string()
        };
        sample_values.push(str_value);
    }

    for value in values {
        let str_value = value.to_string();
        if matches!(value, Data::Empty) {
            null_count += 1;
        } else {
            seen_values.insert(str_value.clone());
            
            if min_value.is_none() || str_value < min_value.as_ref().unwrap().to_string() {
                min_value = Some(str_value.clone());
            }
            if max_value.is_none() || str_value > max_value.as_ref().unwrap().to_string() {
                max_value = Some(str_value);
            }
        }
    }

    ColumnInfo {
        name: name.to_string(),
        data_type: detect_column_type(values),
        sample_values,
        null_count,
        unique_count: seen_values.len(),
        min_value,
        max_value,
        has_duplicates: seen_values.len() < values.len() - null_count,
    }
}

pub async fn analyze_excel_file<P: AsRef<Path>>(path: P) -> Result<SheetAnalysis, AppError> {
    let path_ref = path.as_ref();
    
    tracing::info!("Starting file analysis");
    let start = std::time::Instant::now();

    // Basic file checks
    if !path_ref.exists() {
        return Err(AppError::InvalidInput(format!("File does not exist: {:?}", path_ref)));
    }

    // Open workbook
    let mut workbook: Xlsx<_> = open_workbook(path_ref)
        .map_err(|e| AppError::InvalidInput(format!("Failed to open Excel file: {}", e)))?;
    
    let sheet_names: Vec<String> = workbook.sheet_names().to_vec();
    
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
            
            let headers = rows.first()
                .map(|row| row.iter()
                    .map(|cell| clean_column_name(&cell.to_string()))
                    .collect::<Vec<_>>())
                .unwrap_or_default();

            // Quick column type detection
            let mut date_columns = Vec::new();
            let mut numeric_columns = Vec::new();
            let mut text_columns = Vec::new();
            
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
                    
                    // Convert to strings before counting uniques
                    let string_values: Vec<String> = values.iter()
                        .map(|v| v.to_string())
                        .collect();
                    let unique_values: std::collections::HashSet<_> = string_values.iter().collect();
                    
                    ColumnInfo {
                        name: name.clone(),
                        data_type,
                        sample_values: values.iter().take(5).map(|v| v.to_string()).collect(),
                        null_count: values.iter().filter(|v| matches!(v, Data::Empty)).count(),
                        unique_count: unique_values.len(),
                        min_value: None, // Skip min/max for performance
                        max_value: None,
                        has_duplicates: false, // Skip duplicate check for performance
                    }
                })
                .collect();

            tracing::info!("Analysis completed FILEPROCESSOR 216 in {:?}", start.elapsed());
            
            let df = create_dataframe(&rows, &headers)?;
            
            Ok(SheetAnalysis {
                sheet_names,
                row_count,
                column_count,
                sample_data,
                column_info,
                dataframe: Some(df),
                date_columns,
                numeric_columns,
                text_columns,
            })
        } else {
            Err(AppError::InvalidInput("Failed to read worksheet".to_string()))
        }
    } else {
        Err(AppError::InvalidInput("No sheets found in workbook".to_string()))
    }
}

fn clean_column_name(name: &str) -> String {
    let cleaned = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect::<String>()
        .to_lowercase();
    
    if cleaned.chars().next().map_or(true, |c| !c.is_alphabetic()) {
        format!("col_{}", cleaned)
    } else {
        cleaned
    }
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

pub async fn process_csv_file(file_data: Bytes, db_loader: &DbLoader) -> Result<u32, AppError> {
    tracing::info!("Processing CSV file");
    let cursor = Cursor::new(file_data);
    
    let df = CsvReader::new(cursor)
        .infer_schema(Some(100))
        .has_header(true)
        .finish()
        .map_err(|e| AppError::FileProcessingError(format!("Failed to read CSV: {}", e)))?;

    let mut df = clean_dataframe(&df)
        .ok_or_else(|| AppError::FileProcessingError("CSV file is empty after cleaning".to_string()))?;

    // Detect and normalize date columns
    let date_columns = detect_date_columns(&df);
    df = normalize_date_columns(&mut df, &date_columns);

    // Generate a unique table name
    let table_name = format!("csv_data_{}", chrono::Utc::now().timestamp());
    let clean_table_name = clean_table_name(&table_name);

    // Load the data into SQLite
    db_loader.load_dataframe(df, &clean_table_name).await?;

    Ok(1) // Return 1 for one table processed
}

pub async fn process_excel_file(file_data: Bytes, _file_extension: &str, db_loader: &DbLoader) -> Result<u32, AppError> {
    tracing::info!("Processing Excel file");
    let cursor = Cursor::new(file_data);
    
    let mut workbook: Xlsx<_> = open_workbook_from_rs(cursor)
        .map_err(|e| AppError::FileProcessingError(format!("Failed to open Excel file: {}", e)))?;

    let mut total_tabs = 0;
    let sheet_names = workbook.sheet_names().to_vec();

    for sheet_name in &sheet_names {
        match workbook.worksheet_range(sheet_name) {
            Ok(range) => {
                let rows: Vec<Vec<Data>> = range.rows().map(|row| row.to_vec()).collect();
                
                if rows.is_empty() {
                    continue;
                }

                let headers = rows.first()
                    .map(|row| row.iter()
                        .map(|cell| clean_column_name(&cell.to_string()))
                        .collect::<Vec<_>>())
                    .unwrap_or_default();

                if let Ok(mut df) = create_dataframe(&rows, &headers) {
                    if let Some(cleaned_df) = clean_dataframe(&df) {
                        df = cleaned_df;
                        
                        // Detect and normalize date columns
                        let date_columns = detect_date_columns(&df);
                        df = normalize_date_columns(&mut df, &date_columns);

                        // Generate a unique table name
                        let table_name = format!("excel_{}_{}", clean_table_name(sheet_name), chrono::Utc::now().timestamp());
                        
                        // Load the data into SQLite
                        if let Ok(()) = db_loader.load_dataframe(df, &table_name).await {
                            total_tabs += 1;
                        }
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
        Err(AppError::FileProcessingError("No valid data found in Excel file".to_string()))
    } else {
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

pub async fn process_file_from_url(url: &str, db_loader: &DbLoader) -> Result<u32, AppError> {
    let file_extension = url
        .split('.')
        .last()
        .ok_or_else(|| AppError::FileProcessingError("Invalid file URL".to_string()))?;

    let file_data = load_file_from_url(url).await?;

    match file_extension.to_lowercase().as_str() {
        "csv" => process_csv_file(file_data, db_loader).await,
        "xlsx" | "xls" => process_excel_file(file_data, file_extension, db_loader).await,
        _ => Err(AppError::FileProcessingError("Unsupported file type".to_string()))
    }
}

pub async fn analyze_excel_file_from_url(url: &str) -> Result<SheetAnalysis, AppError> {
    tracing::info!("Starting file analysis from URL");
    let start = std::time::Instant::now();

    let file_data = load_file_from_url(url).await?;
    let cursor = Cursor::new(file_data);
    
    // Open workbook
    let mut workbook: Xlsx<_> = open_workbook_from_rs(cursor)
        .map_err(|e| AppError::FileProcessingError(format!("Failed to open Excel file: {}", e)))?;
    
    let sheet_names: Vec<String> = workbook.sheet_names().to_vec();
    
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
            
            let headers = rows.first()
                .map(|row| row.iter()
                    .map(|cell| clean_column_name(&cell.to_string()))
                    .collect::<Vec<_>>())
                .unwrap_or_default();

            // Quick column type detection
            let mut date_columns = Vec::new();
            let mut numeric_columns = Vec::new();
            let mut text_columns = Vec::new();
            
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

pub async fn process_file(file_bytes: &[u8], _file_extension: &str, db_loader: &DbLoader) -> Result<u32, AppError> {
    tracing::info!("Processing file");
    let cursor = Cursor::new(file_bytes);
    
    let df = CsvReader::new(cursor)
        .infer_schema(Some(100))
        .has_header(true)
        .finish()
        .map_err(|e| AppError::FileProcessingError(format!("Failed to read CSV: {}", e)))?;

    let mut df = clean_dataframe(&df)
        .ok_or_else(|| AppError::FileProcessingError("CSV file is empty after cleaning".to_string()))?;

    // Detect and normalize date columns
    let date_columns = detect_date_columns(&df);
    df = normalize_date_columns(&mut df, &date_columns);

    // Generate a unique table name
    let table_name = format!("csv_data_{}", chrono::Utc::now().timestamp());
    let clean_table_name = clean_table_name(&table_name);

    // Load the data into SQLite
    db_loader.load_dataframe(df, &clean_table_name).await?;

    Ok(1) // Return 1 for one table processed
}