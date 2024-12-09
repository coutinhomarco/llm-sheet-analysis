use tokio_rusqlite::Connection;
use tokio::sync::Mutex;
use moka::sync::Cache;
use polars::prelude::*;
use crate::error::AppError;
use tracing::{info, debug, warn};
use std::time::Duration;
use rusqlite::types::ValueRef;
use std::sync::Arc;

const BATCH_SIZE: usize = 300;
const CACHE_TTL: Duration = Duration::from_secs(3600); // 1 hour
const CACHE_CAPACITY: u64 = 100;

#[derive(Clone)]
pub struct DbLoader {
    conn: Arc<Mutex<Connection>>,
    cache: Cache<String, DataFrame>,
    current_table: Arc<Mutex<Option<String>>>,
    column_names: Arc<Mutex<Vec<String>>>,
}

impl DbLoader {
    pub async fn new() -> Result<Self, AppError> {
        info!("Creating new DbLoader instance");
        let conn = Connection::open_in_memory()
            .await
            .map_err(|e| AppError::DatabaseError(e.to_string()))?;

        let cache = Cache::builder()
            .max_capacity(CACHE_CAPACITY)
            .time_to_live(CACHE_TTL)
            .build();

        debug!("Successfully created connection and cache");
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            cache,
            current_table: Arc::new(Mutex::new(None)),
            column_names: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub async fn load_dataframe(&self, df: DataFrame, table_name: &str) -> Result<(), AppError> {
        info!("Loading DataFrame into table: {}", table_name);
        debug!("DataFrame shape: {} rows x {} columns", df.height(), df.width());

        // Update current_table and column_names BEFORE database operations
        {
            *self.current_table.lock().await = Some(table_name.to_string());
            *self.column_names.lock().await = df.get_column_names()
                .iter()
                .map(|&s| s.to_string())
                .collect();
        }

        // Cache the DataFrame
        self.cache.insert(table_name.to_string(), df.clone());
        
        let conn = self.conn.lock().await;
        let df = df.clone();
        let table_name = table_name.to_string();
        let this = self.clone();
        
        conn.call(move |conn: &mut rusqlite::Connection| -> rusqlite::Result<()> {
            let tx = conn.transaction()?;
            
            // Drop existing table if it exists
            let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
            tx.execute(&drop_sql, [])?;

            // Create table schema
            let schema = df.schema();
            let create_table_sql = this.generate_create_table_sql(&table_name, &schema)
                .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;
            tx.execute(&create_table_sql, [])?;

            // Generate simpler insert SQL with just one row of placeholders
            let columns = df.get_column_names();
            let placeholders = vec!["?"; df.width()].join(", ");
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name,
                columns.join(", "),
                placeholders
            );

            {
                let mut stmt = tx.prepare(&insert_sql)?;
                
                // Process in batches
                let total_rows = df.height();
                for chunk_start in (0..total_rows).step_by(BATCH_SIZE) {
                    let chunk_end = (chunk_start + BATCH_SIZE).min(total_rows);
                    debug!("Processing batch {}-{}/{}", chunk_start, chunk_end, total_rows);

                    for row_idx in chunk_start..chunk_end {
                        let params = this.prepare_row_params(&df, row_idx)
                            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;
                        let param_refs: Vec<&dyn rusqlite::ToSql> = params
                            .iter()
                            .map(|p| p as &dyn rusqlite::ToSql)
                            .collect();

                        stmt.execute(param_refs.as_slice())?;
                    }
                }
            }
            
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| AppError::DatabaseError(e.to_string()))
    }

    fn prepare_row_params(&self, df: &DataFrame, row_idx: usize) -> Result<Vec<Box<dyn rusqlite::ToSql>>, AppError> {
        let mut params = Vec::with_capacity(df.width());
        for series in df.get_columns() {
            let value = match series.get(row_idx) {
                Ok(value) => match value {
                    AnyValue::Null => Box::new(rusqlite::types::Null) as Box<dyn rusqlite::ToSql>,
                    AnyValue::Int32(v) => Box::new(v) as Box<dyn rusqlite::ToSql>,
                    AnyValue::Int64(v) => Box::new(v) as Box<dyn rusqlite::ToSql>,
                    AnyValue::Float32(v) => Box::new(v as f64) as Box<dyn rusqlite::ToSql>,
                    AnyValue::Float64(v) => Box::new(v) as Box<dyn rusqlite::ToSql>,
                    AnyValue::String(v) => Box::new(v.to_string()) as Box<dyn rusqlite::ToSql>,
                    AnyValue::Boolean(v) => Box::new(v) as Box<dyn rusqlite::ToSql>,
                    _ => Box::new(value.to_string()) as Box<dyn rusqlite::ToSql>,
                },
                Err(e) => {
                    warn!("Error getting value at row {}: {}", row_idx, e);
                    Box::new(rusqlite::types::Null) as Box<dyn rusqlite::ToSql>
                }
            };
            params.push(value);
        }
        Ok(params)
    }

    pub async fn get_schema_with_samples(&self) -> Result<String, AppError> {
        if !self.has_data().await {
            warn!("Attempted to get schema before loading any data");
            return Ok("No data has been loaded into the database yet".to_string());
        }
        
        let conn = self.conn.lock().await;
        
        conn.call(|conn: &mut rusqlite::Connection| -> rusqlite::Result<String> {
            // Get all tables
            debug!("Querying for all tables");
            let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
            let tables: Vec<String> = stmt.query_map([], |row| row.get(0))?
                .filter_map(Result::ok)
                .collect();

            debug!("Found tables: {:?}", tables);
            
            if tables.is_empty() {
                warn!("No tables found in database");
                return Ok("No tables found in database".to_string());
            }

            let mut schema = String::new();
            for table in tables {
                info!("Processing table: {}", table);
                schema.push_str(&format!("\nTable: {}\n", table));
                
                // Get column info
                let pragma_sql = format!("PRAGMA table_info('{}')", table);
                debug!("Getting column info with: {}", pragma_sql);
                
                let mut stmt = conn.prepare(&pragma_sql)?;
                let cols: Vec<(String, String)> = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(1)?, // column name
                        row.get::<_, String>(2)?, // data type
                    ))
                })?
                .filter_map(Result::ok)
                .collect();

                debug!("Found columns: {:?}", cols);
                
                schema.push_str("Columns:\n");
                for col in cols {
                    schema.push_str(&format!("  {} {}\n", col.0, col.1));
                }

                // Get sample data
                let sample_sql = format!("SELECT * FROM '{}' LIMIT 3", table);
                let mut stmt = conn.prepare(&sample_sql)?;
                let mut rows = stmt.query([])?;
                
                schema.push_str("\nSample Data:\n");
                while let Some(row) = rows.next()? {
                    let mut row_data = Vec::new();
                    let column_count = row.as_ref().column_count();
                    
                    for i in 0..column_count {
                        let value = match row.get_ref(i)? {
                            ValueRef::Null => "NULL".to_string(),
                            ValueRef::Integer(i) => i.to_string(),
                            ValueRef::Real(f) => f.to_string(),
                            ValueRef::Text(t) => format!("'{}'", String::from_utf8_lossy(t)),
                            ValueRef::Blob(_) => "BLOB".to_string(),
                        };
                        row_data.push(value);
                    }
                    schema.push_str(&format!("  {}\n", row_data.join(", ")));
                }
                schema.push_str("\n");
            }

            info!("Successfully generated schema");
            debug!("Final schema: {}", schema);
            Ok(schema)
        })
        .await
        .map_err(|e| AppError::DatabaseError(e.to_string()))
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

    // Add a new method to check if data is loaded
    pub async fn has_data(&self) -> bool {
        let has_table = self.current_table.lock()
            .await
            .is_some();
        
        let has_columns = !self.column_names.lock()
            .await
            .is_empty();

        tracing::debug!(
            "has_data check - has_table: {}, has_columns: {}", 
            has_table, 
            has_columns
        );
        
        has_table && has_columns
    }

    pub async fn get_connection(&self) -> Result<tokio::sync::MutexGuard<'_, Connection>, AppError> {
        match self.conn.lock().await {
            guard => Ok(guard)
        }
    }
}

