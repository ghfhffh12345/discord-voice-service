use discord_voice_service::api::service::ControlService;
use discord_voice_service::config::Settings;
use discord_voice_service::session::supervisor::Supervisor;

#[tokio::main]
async fn main() {
    let _settings = Settings::from_env().expect("settings");
    let _service = ControlService {
        supervisor: Supervisor::new(),
    };
}
