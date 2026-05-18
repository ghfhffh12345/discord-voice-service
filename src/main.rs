use discord_voice_service::config::Settings;
use tonic::transport::Server;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let settings = Settings::from_env()?;
    let (_health_reporter, health_service) = tonic_health::server::health_reporter();

    Server::builder()
        .add_service(health_service)
        .serve(settings.listen_addr)
        .await?;

    Ok(())
}
