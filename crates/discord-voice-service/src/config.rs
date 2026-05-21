use std::{collections::HashMap, env::VarError, net::SocketAddr, time::Duration};

use discord_voice_service_runtime::RuntimeError;

const MISSING_BIND_ADDR: &str = "missing DISCORD_VOICE_SERVICE_BIND_ADDR";
const INVALID_BIND_ADDR: &str = "invalid DISCORD_VOICE_SERVICE_BIND_ADDR";
const MISSING_YTMUSIC_ADDR: &str = "missing DISCORD_VOICE_SERVICE_YTMUSIC_ADDR";
const INVALID_YTMUSIC_ADDR: &str = "invalid DISCORD_VOICE_SERVICE_YTMUSIC_ADDR";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    pub bind_addr: SocketAddr,
    pub ytmusic_addr: String,
    pub prebuffer_frames: usize,
    pub max_buffer_frames: usize,
}

impl Settings {
    pub fn from_env() -> Result<Self, RuntimeError> {
        let bind_addr = read_env("DISCORD_VOICE_SERVICE_BIND_ADDR")?;
        let ytmusic_addr = read_env("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR")?;
        Self::from_values(&bind_addr, &ytmusic_addr)
    }

    pub fn from_pairs<const N: usize>(pairs: [(&str, &str); N]) -> Result<Self, RuntimeError> {
        let env = pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect::<HashMap<_, _>>();
        Self::from_map(&env)
    }

    pub fn ytmusic_probe_interval(&self) -> Duration {
        Duration::from_secs(15)
    }

    pub fn ytmusic_probe_timeout(&self) -> Duration {
        Duration::from_secs(3)
    }

    fn from_values(bind_addr: &str, ytmusic_addr: &str) -> Result<Self, RuntimeError> {
        let bind_addr = bind_addr
            .parse()
            .map_err(|_| RuntimeError::InvalidState(INVALID_BIND_ADDR))?;
        validate_ytmusic_addr(ytmusic_addr)?;

        Ok(Self {
            bind_addr,
            ytmusic_addr: ytmusic_addr.to_owned(),
            prebuffer_frames: 150,
            max_buffer_frames: 300,
        })
    }

    fn from_map(env: &HashMap<String, String>) -> Result<Self, RuntimeError> {
        let bind_addr = env
            .get("DISCORD_VOICE_SERVICE_BIND_ADDR")
            .ok_or(RuntimeError::InvalidState(MISSING_BIND_ADDR))?;
        let ytmusic_addr = env
            .get("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR")
            .cloned()
            .ok_or(RuntimeError::InvalidState(MISSING_YTMUSIC_ADDR))?;

        Self::from_values(bind_addr, &ytmusic_addr)
    }
}

fn read_env(key: &'static str) -> Result<String, RuntimeError> {
    match std::env::var(key) {
        Ok(value) => Ok(value),
        Err(VarError::NotPresent) => Err(RuntimeError::InvalidState(missing_env_error(key))),
        Err(VarError::NotUnicode(_)) => Err(RuntimeError::InvalidState(invalid_env_error(key))),
    }
}

fn validate_ytmusic_addr(value: &str) -> Result<(), RuntimeError> {
    let uri: http::Uri = value
        .parse()
        .map_err(|_| RuntimeError::InvalidState(INVALID_YTMUSIC_ADDR))?;

    match uri.scheme_str() {
        Some("http") | Some("https") if uri.authority().is_some() => Ok(()),
        _ => Err(RuntimeError::InvalidState(INVALID_YTMUSIC_ADDR)),
    }
}

fn missing_env_error(key: &'static str) -> &'static str {
    match key {
        "DISCORD_VOICE_SERVICE_BIND_ADDR" => MISSING_BIND_ADDR,
        "DISCORD_VOICE_SERVICE_YTMUSIC_ADDR" => MISSING_YTMUSIC_ADDR,
        _ => "missing required environment variable",
    }
}

fn invalid_env_error(key: &'static str) -> &'static str {
    match key {
        "DISCORD_VOICE_SERVICE_BIND_ADDR" => INVALID_BIND_ADDR,
        "DISCORD_VOICE_SERVICE_YTMUSIC_ADDR" => INVALID_YTMUSIC_ADDR,
        _ => "invalid environment variable",
    }
}
