use serde::Deserialize;
use anyhow::Result;
use dotenvy::dotenv;

fn default_max_file_size() -> usize {
    // 10 MB in bytes
    10 * 1024 * 1024
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub max_file_size: usize,
    pub openai_key: String,
}

impl Config {
    pub fn new() -> Result<Self> {
        // Load .env file first
        dotenv().ok();

        // Then load the OpenAI key
        let openai_key = std::env::var("OPENAI_API_KEY")
            .map_err(|e| anyhow::anyhow!("Failed to load OPENAI_API_KEY: {}", e))?;

        Ok(Config {
            max_file_size: 10 * 1024 * 1024, // 10MB
            openai_key,
        })
    }
}

