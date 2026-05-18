use discord_voice_service::config::Settings;

fn main() {
    let _ = Settings::from_env();
}
