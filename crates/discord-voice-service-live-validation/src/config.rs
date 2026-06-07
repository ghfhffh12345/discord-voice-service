use std::{collections::HashMap, env, str::FromStr};

use anyhow::{Context, Result, bail};
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker},
};

pub const LIVE_STAGING_PROFILE_CONSTRAINED_GITHUB: &str = "constrained-github-hosted";
pub const LIVE_STAGING_PROFILE_CONSTRAINED_LOCAL: &str = "constrained-local";
pub const MAX_LIVE_STAGING_SERVICE_CPUS: f64 = 1.0;
pub const MIN_LIVE_STAGING_CPU_CONTENTION_WORKERS: u64 = 2;
pub const MIN_LIVE_STAGING_HTTP_READ_DELAY_MS: u64 = 5;
pub const MIN_LIVE_STAGING_HTTP_READ_JITTER_MS: u64 = 25;

#[derive(Clone, PartialEq, Eq)]
pub struct StagingConfig {
    pub application_id: String,
    pub bot_token: String,
    pub observer_bot_token: String,
    pub test_guild_id: String,
    pub test_voice_channel_id: String,
    pub test_video_id: String,
    pub discord_voice_service_uri: String,
    pub discord_voice_service_ytmusic_addr: String,
    pub live_staging_profile: String,
    pub live_staging_service_cpus: String,
    pub live_staging_cpu_contention_workers: u64,
    pub live_staging_http_read_delay_ms: u64,
    pub live_staging_http_read_jitter_ms: u64,
}

impl StagingConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_env_map(env::vars().collect())
    }

    pub fn from_env_map(env: HashMap<String, String>) -> Result<Self> {
        let config = Self {
            observer_bot_token: required_env(&env, "OBSERVER_BOT_TOKEN")?,
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
            live_staging_profile: required_env(&env, "LIVE_STAGING_PROFILE")?,
            live_staging_service_cpus: required_env(&env, "LIVE_STAGING_SERVICE_CPUS")?,
            live_staging_cpu_contention_workers: required_u64_env(
                &env,
                "LIVE_STAGING_CPU_CONTENTION_WORKERS",
            )?,
            live_staging_http_read_delay_ms: required_u64_env(
                &env,
                "LIVE_STAGING_HTTP_READ_DELAY_MS",
            )?,
            live_staging_http_read_jitter_ms: required_u64_env(
                &env,
                "LIVE_STAGING_HTTP_READ_JITTER_MS",
            )?,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn guild_id(&self) -> Result<Id<GuildMarker>> {
        parse_id(&self.test_guild_id, "TEST_GUILD_ID")
    }

    pub fn channel_id(&self) -> Result<Id<ChannelMarker>> {
        parse_id(&self.test_voice_channel_id, "TEST_VOICE_CHANNEL_ID")
    }

    fn validate(&self) -> Result<()> {
        if !matches!(
            self.live_staging_profile.as_str(),
            LIVE_STAGING_PROFILE_CONSTRAINED_GITHUB | LIVE_STAGING_PROFILE_CONSTRAINED_LOCAL
        ) {
            bail!(
                "LIVE_STAGING_PROFILE must be `{LIVE_STAGING_PROFILE_CONSTRAINED_GITHUB}` or `{LIVE_STAGING_PROFILE_CONSTRAINED_LOCAL}`; got `{}`",
                self.live_staging_profile
            );
        }

        let service_cpus: f64 = self.live_staging_service_cpus.parse().with_context(|| {
            format!(
                "LIVE_STAGING_SERVICE_CPUS must be a positive decimal no greater than {MAX_LIVE_STAGING_SERVICE_CPUS}"
            )
        })?;
        if !service_cpus.is_finite()
            || service_cpus <= 0.0
            || service_cpus > MAX_LIVE_STAGING_SERVICE_CPUS
        {
            bail!(
                "LIVE_STAGING_SERVICE_CPUS must be > 0 and <= {MAX_LIVE_STAGING_SERVICE_CPUS}; got `{}`",
                self.live_staging_service_cpus
            );
        }

        if self.live_staging_cpu_contention_workers < MIN_LIVE_STAGING_CPU_CONTENTION_WORKERS {
            bail!(
                "LIVE_STAGING_CPU_CONTENTION_WORKERS must be at least {MIN_LIVE_STAGING_CPU_CONTENTION_WORKERS}; got {}",
                self.live_staging_cpu_contention_workers
            );
        }
        if self.live_staging_http_read_delay_ms < MIN_LIVE_STAGING_HTTP_READ_DELAY_MS {
            bail!(
                "LIVE_STAGING_HTTP_READ_DELAY_MS must be at least {MIN_LIVE_STAGING_HTTP_READ_DELAY_MS}; got {}",
                self.live_staging_http_read_delay_ms
            );
        }
        if self.live_staging_http_read_jitter_ms < MIN_LIVE_STAGING_HTTP_READ_JITTER_MS {
            bail!(
                "LIVE_STAGING_HTTP_READ_JITTER_MS must be at least {MIN_LIVE_STAGING_HTTP_READ_JITTER_MS}; got {}",
                self.live_staging_http_read_jitter_ms
            );
        }
        Ok(())
    }
}

impl std::fmt::Debug for StagingConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagingConfig")
            .field("application_id", &self.application_id)
            .field("bot_token", &"[REDACTED]")
            .field("observer_bot_token", &"[REDACTED]")
            .field("test_guild_id", &self.test_guild_id)
            .field("test_voice_channel_id", &self.test_voice_channel_id)
            .field("test_video_id", &self.test_video_id)
            .field("discord_voice_service_uri", &self.discord_voice_service_uri)
            .field(
                "discord_voice_service_ytmusic_addr",
                &self.discord_voice_service_ytmusic_addr,
            )
            .field("live_staging_profile", &self.live_staging_profile)
            .field("live_staging_service_cpus", &self.live_staging_service_cpus)
            .field(
                "live_staging_cpu_contention_workers",
                &self.live_staging_cpu_contention_workers,
            )
            .field(
                "live_staging_http_read_delay_ms",
                &self.live_staging_http_read_delay_ms,
            )
            .field(
                "live_staging_http_read_jitter_ms",
                &self.live_staging_http_read_jitter_ms,
            )
            .finish()
    }
}

pub fn required_env(env: &HashMap<String, String>, key: &'static str) -> Result<String> {
    match env.get(key).map(String::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value.to_owned()),
        Some(_) => bail!("required env var {key} must not be empty"),
        None => bail!("missing required env var: {key}"),
    }
}

pub fn required_u64_env(env: &HashMap<String, String>, key: &'static str) -> Result<u64> {
    let value = required_env(env, key)?;
    value
        .parse()
        .with_context(|| format!("env var {key} must be an unsigned integer"))
}

pub fn parse_id<T>(value: &str, field: &'static str) -> Result<Id<T>> {
    Id::<T>::from_str(value).with_context(|| format!("invalid Discord snowflake in {field}"))
}
