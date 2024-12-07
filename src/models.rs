use polars::frame::DataFrame;
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