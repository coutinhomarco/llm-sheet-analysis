use rusqlite::{Connection, types::ValueRef};
use polars::prelude::*;
use crate::error::AppError;
use std::sync::Mutex;
use tracing::{info, debug, error, warn};

pub struct DbLoader {
    conn: Mutex<Connection>,
    current_table: Mutex<Option<String>>,
    column_names: Mutex<Vec<String>>,
}
impl DbLoader {
    pub fn new() -> Result<Self, AppError> {
        info!("Creating new DbLoader instance");
        let conn = Connection::open_in_memory()
            .map_err(|e| {
                error!("Failed to open in-memory database: {}", e);
                AppError::DatabaseError(e.to_string())
            })?;
        
        debug!("Successfully created in-memory database connection");
        Ok(Self {
            conn: Mutex::new(conn),
            current_table: Mutex::new(None),
            column_names: Mutex::new(Vec::new()),
        })
    }

    pub async fn load_dataframe(&self, df: DataFrame, table_name: &str) -> Result<(), AppError> {
        info!("Loading DataFrame into table: {}", table_name);
        debug!("DataFrame shape: {} rows x {} columns", df.height(), df.width());
        
        // Store column names before processing
        let column_names: Vec<String> = df.get_column_names()
            .iter()
            .map(|&s| s.to_string())
            .collect();
        
        let conn = self.conn.lock().map_err(|e| {
            error!("Failed to acquire database lock: {}", e);
            AppError::DatabaseError(e.to_string())
        })?;
        
        // Drop existing table if it exists
        let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
        conn.execute(&drop_sql, []).map_err(|e| {
            error!("Failed to drop existing table: {}", e);
            AppError::DatabaseError(e.to_string())
        })?;
        
        // Create table schema
        let schema = df.schema();
        debug!("Generated schema: {:?}", schema);
        
        let create_table_sql = self.generate_create_table_sql(table_name, &schema)?;
        debug!("Create table SQL: {}", create_table_sql);
        
        conn.execute(&create_table_sql, []).map_err(|e| {
            error!("Failed to create table: {}", e);
            AppError::DatabaseError(e.to_string())
        })?;
        
        // Generate insert SQL statement
        let insert_sql = self.generate_insert_sql(table_name, &df)?;
        debug!("Insert SQL template: {}", insert_sql);
        
        // Prepare the statement
        let mut stmt = conn.prepare(&insert_sql).map_err(|e| {
            error!("Failed to prepare insert statement: {}", e);
            AppError::DatabaseError(e.to_string())
        })?;
    
        // Process row by row
        info!("Starting row insertion for {} rows", df.height());
        for row_idx in 0..df.height() {
            if row_idx % 100 == 0 {
                debug!("Processing row {}/{}", row_idx, df.height());
            }
            
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
                            _ => rusqlite::types::ToSqlOutput::from(value.to_string()),
                        },
                        Err(e) => {
                            warn!("Error getting value at row {}: {}", row_idx, e);
                            rusqlite::types::ToSqlOutput::from(rusqlite::types::Null)
                        }
                    }
                })
                .collect();
    
            let param_refs: Vec<&dyn rusqlite::ToSql> = params
                .iter()
                .map(|p| p as &dyn rusqlite::ToSql)
                .collect();
    
            if let Err(e) = stmt.execute(param_refs.as_slice()) {
                error!("Failed to insert row {}: {}", row_idx, e);
                return Err(AppError::DatabaseError(e.to_string()));
            }
        }
    
        info!("Successfully loaded DataFrame into table {}", table_name);
        
        // Verify table was created
        let verify_sql = format!("SELECT COUNT(*) FROM {}", table_name);
        let count: i64 = conn.query_row(&verify_sql, [], |row| row.get(0)).map_err(|e| {
            error!("Failed to verify table creation: {}", e);
            AppError::DatabaseError(e.to_string())
        })?;
        
        info!("Verified table {} contains {} rows", table_name, count);
        
        // Update the struct's state using the Mutex guards
        {
            let mut current_table = self.current_table.lock().map_err(|e| {
                error!("Failed to acquire current_table lock: {}", e);
                AppError::DatabaseError(e.to_string())
            })?;
            *current_table = Some(table_name.to_string());

            let mut stored_column_names = self.column_names.lock().map_err(|e| {
                error!("Failed to acquire column_names lock: {}", e);
                AppError::DatabaseError(e.to_string())
            })?;
            *stored_column_names = column_names;
        }

        Ok(())
    }
    pub async fn get_schema_with_samples(&self) -> Result<String, AppError> {
        if !self.has_data() {
            warn!("Attempted to get schema before loading any data");
            return Ok("No data has been loaded into the database yet".to_string());
        }
        
        info!("Getting schema with samples");
        let conn = self.conn.lock().map_err(|e| {
            error!("Failed to acquire database lock: {}", e);
            AppError::DatabaseError(e.to_string())
        })?;
        
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

    pub async fn get_database_schema(&self) -> Result<String, AppError> {
        let conn = self.conn.lock()
            .map_err(|e| AppError::DatabaseError(e.to_string()))?;
        let mut schema = String::new();
        
        // Get all tables
        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
        let tables: Vec<String> = stmt.query_map([], |row| row.get(0))?
            .filter_map(Result::ok)
            .collect();

        for table in tables {
            schema.push_str(&format!("\nTable: {}\n", table));
            
            // Get column info
            let pragma_sql = format!("PRAGMA table_info('{}')", table);
            let mut stmt = conn.prepare(&pragma_sql)?;
            let cols = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?, // column name
                    row.get::<_, String>(2)?, // data type
                ))
            })?;

            for col in cols {
                if let Ok((name, type_)) = col {
                    schema.push_str(&format!("  - {}: {}\n", name, type_));
                }
            }
        }

        Ok(schema)
    }

    // Add a new method to check if data is loaded
    pub fn has_data(&self) -> bool {
        let has_table = self.current_table.lock()
            .map(|guard| guard.is_some())
            .unwrap_or(false);
        
        let has_columns = self.column_names.lock()
            .map(|guard| !guard.is_empty())
            .unwrap_or(false);
        
        has_table && has_columns
    }
}
