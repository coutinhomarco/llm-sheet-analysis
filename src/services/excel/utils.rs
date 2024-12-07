use std::collections::HashSet;
use bytes::Bytes;
use reqwest::Client;
use crate::error::AppError;
use chrono::NaiveDateTime;
use calamine::Data;

pub fn clean_column_name(name: &str, existing_names: &mut HashSet<String>) -> String {
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

pub fn clean_table_name(name: &str) -> String {
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

pub fn update_min_max(min_max: &mut (Option<String>, Option<String>), value: &str) {
    match &min_max.0 {
        Some(min_val) if value < min_val.as_str() => min_max.0 = Some(value.to_string()),
        None => min_max.0 = Some(value.to_string()),
        _ => {}
    }

    match &min_max.1 {
        Some(max_val) if value > max_val.as_str() => min_max.1 = Some(value.to_string()),
        None => min_max.1 = Some(value.to_string()),
        _ => {}
    }
}

pub fn merge_min_max(
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

pub fn is_date_string(s: &str) -> bool {
    // Common date formats to try
    let formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
    ];

    for format in formats.iter() {
        if NaiveDateTime::parse_from_str(s, format).is_ok() {
            return true;
        }
    }
    false
}

pub fn detect_column_type(values: &[Data]) -> &'static str {
    let mut numeric_count = 0;
    let mut date_count = 0;
    let mut total_count = 0;

    for value in values.iter().filter(|v| !matches!(v, Data::Empty)) {
        total_count += 1;
        match value {
            Data::Float(_) | Data::Int(_) => numeric_count += 1,
            Data::DateTime(_) => date_count += 1,
            Data::String(s) if is_date_string(s) => date_count += 1,
            _ => {}
        }
    }

    if total_count == 0 {
        return "string";
    }

    let numeric_ratio = numeric_count as f64 / total_count as f64;
    let date_ratio = date_count as f64 / total_count as f64;

    if date_ratio > 0.5 {
        "date"
    } else if numeric_ratio > 0.5 {
        "numeric"
    } else {
        "string"
    }
}