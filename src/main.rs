use discord_voice_service::config::Settings;

#[tokio::main]
async fn main() {
    let _settings = Settings::from_env().expect("settings");
}
