use polars::frame::DataFrame;
use smallvec::SmallVec;

#[derive(Debug)]
pub struct SheetAnalysis {
    pub sheet_names: SmallVec<[String; 4]>,
    pub row_count: usize,
    pub column_count: usize,
    pub sample_data: SmallVec<[SmallVec<[String; 10]>; 5]>,
    pub column_info: SmallVec<[ColumnInfo; 32]>,
    pub dataframe: Option<DataFrame>,
    pub date_columns: SmallVec<[String; 8]>,
    pub numeric_columns: SmallVec<[String; 16]>,
    pub text_columns: SmallVec<[String; 16]>,
}

#[derive(Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub sample_values: SmallVec<[String; 5]>,
    pub null_count: usize,
    pub unique_count: usize,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub has_duplicates: bool,
}