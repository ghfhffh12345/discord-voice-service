use std::{collections::HashMap, net::SocketAddr};

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
        let env = std::env::vars().collect::<HashMap<_, _>>();
        Self::from_map(&env)
    }

    pub fn from_pairs<const N: usize>(pairs: [(&str, &str); N]) -> Result<Self, AppError> {
        let env = pairs
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect::<HashMap<_, _>>();
        Self::from_map(&env)
    }

    fn from_map(env: &HashMap<String, String>) -> Result<Self, AppError> {
        let listen_addr = env
            .get("DISCORD_VOICE_SERVICE_ADDR")
            .ok_or(AppError::MissingEnv("DISCORD_VOICE_SERVICE_ADDR"))?
            .parse()
            .map_err(|_| AppError::InvalidEnv("DISCORD_VOICE_SERVICE_ADDR"))?;

        let ytmusic_addr = env
            .get("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR")
            .cloned()
            .ok_or(AppError::MissingEnv("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR"))?;

        Ok(Self {
            listen_addr,
            ytmusic_addr,
            prebuffer_frames: 150,
            max_buffer_frames: 300,
        })
    }
}
