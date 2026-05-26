use std::{collections::HashMap, env, str::FromStr};

use anyhow::{Context, Result, bail};
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingConfig {
    pub application_id: String,
    pub bot_token: String,
    pub test_guild_id: String,
    pub test_voice_channel_id: String,
    pub test_video_id: String,
    pub discord_voice_service_uri: String,
    pub discord_voice_service_ytmusic_addr: String,
}

impl StagingConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_env_map(env::vars().collect())
    }

    pub fn from_env_map(env: HashMap<String, String>) -> Result<Self> {
        Ok(Self {
            bot_token: required_env(&env, "BOT_TOKEN")?,
            application_id: required_env(&env, "APPLICATION_ID")?,
            test_guild_id: required_env(&env, "TEST_GUILD_ID")?,
            test_voice_channel_id: required_env(&env, "TEST_VOICE_CHANNEL_ID")?,
            test_video_id: required_env(&env, "TEST_VIDEO_ID")?,
            discord_voice_service_uri: required_env(&env, "DISCORD_VOICE_SERVICE_URI")?,
            discord_voice_service_ytmusic_addr: required_env(
                &env,
                "DISCORD_VOICE_SERVICE_YTMUSIC_ADDR",
            )?,
        })
    }

    pub fn guild_id(&self) -> Result<Id<GuildMarker>> {
        parse_id(&self.test_guild_id, "TEST_GUILD_ID")
    }

    pub fn channel_id(&self) -> Result<Id<ChannelMarker>> {
        parse_id(&self.test_voice_channel_id, "TEST_VOICE_CHANNEL_ID")
    }
}

pub fn required_env(env: &HashMap<String, String>, key: &'static str) -> Result<String> {
    match env.get(key).map(String::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value.to_owned()),
        Some(_) => bail!("required env var {key} must not be empty"),
        None => bail!("missing required env var: {key}"),
    }
}

pub fn parse_id<T>(value: &str, field: &'static str) -> Result<Id<T>> {
    Id::<T>::from_str(value).with_context(|| format!("invalid Discord snowflake in {field}"))
}
