use anyhow::Result;
use discord_voice_service::config::Settings;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let settings = Settings::from_env()?;
    discord_voice_service::run(settings).await
}
