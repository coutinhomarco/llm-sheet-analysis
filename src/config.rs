use serde::Deserialize;
use anyhow::Result;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_max_file_size")]
    pub max_file_size: usize,
}

fn default_max_file_size() -> usize {
    10 * 1024 * 1024 // 10MB
}

pub fn load_config() -> Result<Config> {
    // Load .env file if it exists
    dotenvy::dotenv().ok();

    Ok(Config {
        max_file_size: default_max_file_size(),
    })
}