use anyhow::Result;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn init_logging() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    Ok(())
}
