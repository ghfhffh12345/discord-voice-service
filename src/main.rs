use discord_voice_service::config::Settings;

fn main() -> Result<(), discord_voice_service::error::AppError> {
    let _settings = Settings::from_env()?;
    Ok(())
}
