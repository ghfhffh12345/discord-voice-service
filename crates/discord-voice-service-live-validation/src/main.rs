use anyhow::Result;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use discord_voice_service_live_validation::{StagingConfig, run};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    run(StagingConfig::from_env()?).await
}
