use discord_voice_service::api::service::ControlService;
use discord_voice_service::config::Settings;
use discord_voice_service::playback::worker::PlaybackWorker;
use discord_voice_service::session::supervisor::Supervisor;
use discord_voice_service::ytmusic::client::YtMusicClient;

#[tokio::main]
async fn main() {
    let settings = Settings::from_env().expect("settings");
    let supervisor = Supervisor::new();
    let _playback_worker = PlaybackWorker::new(YtMusicClient::new(settings.ytmusic_addr.clone()));
    let _service = ControlService { supervisor };
}
