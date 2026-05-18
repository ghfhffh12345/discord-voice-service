use discord_voice_service::api::service::ControlService;
use discord_voice_service::config::Settings;
use discord_voice_service::proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControlServer;
use discord_voice_service::session::supervisor::Supervisor;
use tonic::transport::Server;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let settings = Settings::from_env()?;
    let supervisor = Supervisor::new();
    let control_service = ControlService { supervisor };
    let (_health_reporter, health_service) = tonic_health::server::health_reporter();

    Server::builder()
        .add_service(health_service)
        .add_service(DiscordVoiceControlServer::new(control_service))
        .serve(settings.listen_addr)
        .await?;

    Ok(())
}
