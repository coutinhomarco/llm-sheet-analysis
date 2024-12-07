use rusqlite::Connection;
use polars::prelude::*;
use crate::error::AppError;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct DbLoader {
    conn: Arc<Mutex<Connection>>
}

impl DbLoader {
    pub fn new() -> Result<Self, AppError> {
        let conn = Connection::open_in_memory()?;
        // Performance optimizations
        conn.execute_batch("
            PRAGMA journal_mode=MEMORY;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=MEMORY;
            PRAGMA cache_size=10000;
        ")?;
        
        Ok(Self { 
            conn: Arc::new(Mutex::new(conn))
        })
    }

    pub async fn load_dataframe(&self, df: DataFrame, table_name: &str) -> Result<(), AppError> {
        let conn = self.conn.lock().await;
        
        // Create table schema
        let schema = df.schema();
        let create_table_sql = self.generate_create_table_sql(table_name, &schema)?;
        conn.execute(&create_table_sql, [])?;

        // Generate insert SQL statement
        let insert_sql = self.generate_insert_sql(table_name, &df)?;
        
        // Prepare the statement once for better performance
        let mut stmt = conn.prepare(&insert_sql)?;

        // Process row by row
        for row_idx in 0..df.height() {
            let params: Vec<rusqlite::types::ToSqlOutput> = df
                .get_columns()
                .iter()
                .map(|series| {
                    match series.get(row_idx) {
                        Ok(value) => match value {
                            AnyValue::Null => rusqlite::types::ToSqlOutput::from(rusqlite::types::Null),
                            AnyValue::Int32(v) => rusqlite::types::ToSqlOutput::from(v),
                            AnyValue::Int64(v) => rusqlite::types::ToSqlOutput::from(v),
                            AnyValue::Float32(v) => rusqlite::types::ToSqlOutput::from(v as f64),
                            AnyValue::Float64(v) => rusqlite::types::ToSqlOutput::from(v),
                            AnyValue::String(v) => rusqlite::types::ToSqlOutput::from(v.to_string()),
                            AnyValue::Boolean(v) => rusqlite::types::ToSqlOutput::from(v),
                            // For any other type, convert to string
                            _ => rusqlite::types::ToSqlOutput::from(value.to_string()),
                        },
                        Err(_) => rusqlite::types::ToSqlOutput::from(rusqlite::types::Null), // Handle error by inserting NULL
                    }
                })
                .collect();

            let param_refs: Vec<&dyn rusqlite::ToSql> = params
                .iter()
                .map(|p| p as &dyn rusqlite::ToSql)
                .collect();

            stmt.execute(param_refs.as_slice())?;
        }

        Ok(())
    }

    pub async fn get_schema_with_samples(&self) -> Result<String, AppError> {
        let conn = self.conn.lock().await;
        let mut schema = Vec::new();

        // Get all tables
        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
        let tables: Vec<String> = stmt.query_map([], |row| row.get(0))?
            .filter_map(Result::ok)
            .collect();

        for table in tables {
            let mut table_info = serde_json::Map::new();
            table_info.insert("table_name".to_string(), table.clone().into());

            // Get column info
            let mut columns = serde_json::Map::new();
            let pragma_sql = format!("PRAGMA table_info('{}')", table);
            let mut stmt = conn.prepare(&pragma_sql)?;
            let cols = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?, // column name
                    row.get::<_, String>(2)?, // column type
                ))
            })?;

            for col in cols.filter_map(Result::ok) {
                columns.insert(col.0, col.1.into());
            }
            table_info.insert("columns".to_string(), columns.into());

            // Get sample rows
            let sample_sql = format!("SELECT * FROM '{}' LIMIT 5", table);
            let mut stmt = conn.prepare(&sample_sql)?;
            let samples = stmt.query_map([], |row| {
                let mut sample = serde_json::Map::new();
                let col_count = row.as_ref().column_count();
                for i in 0..col_count {
                    let col_name = row.as_ref().column_name(i)?;
                    let value: String = row.get(i)?;
                    sample.insert(col_name.to_string(), value.into());
                }
                Ok(sample)
            })?;

            let sample_rows: Vec<serde_json::Value> = samples
                .filter_map(Result::ok)
                .map(serde_json::Value::Object)
                .collect();

            table_info.insert("sample_rows".to_string(), sample_rows.into());
            schema.push(serde_json::Value::Object(table_info));
        }

        Ok(serde_json::to_string_pretty(&schema)?)
    }

    // Helper methods for SQL generation
    fn generate_create_table_sql(&self, table_name: &str, schema: &Schema) -> Result<String, AppError> {
        let columns: Vec<String> = schema
            .iter()
            .map(|(name, dtype)| {
                let sql_type = match dtype {
                    DataType::Int32 | DataType::Int64 => "INTEGER",
                    DataType::Float32 | DataType::Float64 => "REAL",
                    _ => "TEXT",
                };
                format!("{} {}", name, sql_type)
            })
            .collect();
    
        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table_name,
            columns.join(", ")
        ))
    }

    fn generate_insert_sql(&self, table_name: &str, df: &DataFrame) -> Result<String, AppError> {
        let columns = df.get_column_names();
        let placeholders = vec!["?"; columns.len()].join(", ");
        
        Ok(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            columns.join(", "),
            placeholders
        ))
    }
}
