use std::{collections::HashMap, env::VarError, net::SocketAddr};

use crate::error::AppError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    pub listen_addr: SocketAddr,
    pub ytmusic_addr: String,
    pub prebuffer_frames: usize,
    pub max_buffer_frames: usize,
}

impl Settings {
    pub fn from_env() -> Result<Self, AppError> {
        let listen_addr = read_env("DISCORD_VOICE_SERVICE_ADDR")?;
        let ytmusic_addr = read_env("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR")?;
        Self::from_values(&listen_addr, &ytmusic_addr)
    }

    pub fn from_pairs<const N: usize>(pairs: [(&str, &str); N]) -> Result<Self, AppError> {
        let env = pairs
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect::<HashMap<_, _>>();
        Self::from_map(&env)
    }

    fn from_values(listen_addr: &str, ytmusic_addr: &str) -> Result<Self, AppError> {
        let listen_addr = listen_addr
            .parse()
            .map_err(|_| AppError::InvalidEnv("DISCORD_VOICE_SERVICE_ADDR"))?;
        validate_ytmusic_addr(ytmusic_addr)?;

        Ok(Self {
            listen_addr,
            ytmusic_addr: ytmusic_addr.to_owned(),
            prebuffer_frames: 150,
            max_buffer_frames: 300,
        })
    }

    fn from_map(env: &HashMap<String, String>) -> Result<Self, AppError> {
        let listen_addr = env
            .get("DISCORD_VOICE_SERVICE_ADDR")
            .ok_or(AppError::MissingEnv("DISCORD_VOICE_SERVICE_ADDR"))?;

        let ytmusic_addr = env
            .get("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR")
            .cloned()
            .ok_or(AppError::MissingEnv("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR"))?;

        Self::from_values(listen_addr, &ytmusic_addr)
    }
}

fn read_env(key: &'static str) -> Result<String, AppError> {
    match std::env::var(key) {
        Ok(value) => Ok(value),
        Err(VarError::NotPresent) => Err(AppError::MissingEnv(key)),
        Err(VarError::NotUnicode(_)) => Err(AppError::InvalidEnv(key)),
    }
}

fn validate_ytmusic_addr(value: &str) -> Result<(), AppError> {
    let uri: http::Uri = value
        .parse()
        .map_err(|_| AppError::InvalidEnv("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR"))?;

    match uri.scheme_str() {
        Some("http") | Some("https") if uri.authority().is_some() => Ok(()),
        _ => Err(AppError::InvalidEnv("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR")),
    }
}
