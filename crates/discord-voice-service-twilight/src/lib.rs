//! Twilight-first client helpers for `discord-voice-service`.
//!
//! This crate is intentionally a client-side adapter. The service daemon stays
//! gRPC/protobuf-shaped, while Twilight bots get typed Discord IDs, voice state
//! helpers, and gateway-event tracking around the same control API.
//!
//! A typical bot flow is:
//!
//! 1. send [`join_voice_channel`] through a Twilight gateway sender,
//! 2. feed gateway events into [`VoiceContextTracker::observe`],
//! 3. call [`Client::join_voice`] once the tracker returns a [`VoiceContext`],
//! 4. use [`Client::play`], [`Client::pause`], [`Client::resume`], and friends.

use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use tonic::{
    Code, Status,
    transport::{Channel, Endpoint},
};
use twilight_gateway::Event;
use twilight_model::{
    gateway::payload::outgoing::UpdateVoiceState,
    id::{
        Id,
        marker::{ChannelMarker, GuildMarker, UserMarker},
    },
};

use crate::proto::{
    GetStateRequest, JoinVoiceRequest, LeaveVoiceRequest, PauseRequest, PlayRequest, ResumeRequest,
    SessionEvent as ProtoSessionEvent, SessionEventKind as ProtoSessionEventKind,
    SessionEventReason as ProtoSessionEventReason, SessionState as ProtoSessionState,
    SessionStateSnapshot as ProtoSessionStateSnapshot, StopRequest, SubscribeEventsRequest,
    UpdateVoiceContextRequest, discord_voice_control_client::DiscordVoiceControlClient,
    join_voice_request::VoiceContext as ProtoVoiceContext,
};

mod generated {
    pub mod discordvoice {
        pub mod v1 {
            tonic::include_proto!("discordvoice.v1");
        }
    }
}

/// Generated protobuf types for callers that need to interoperate at the raw API boundary.
pub mod proto {
    pub use crate::generated::discordvoice::v1::*;
}

/// A Twilight-oriented gRPC client for `discord-voice-service`.
pub struct Client {
    inner: DiscordVoiceControlClient<Channel>,
}

impl Client {
    /// Connect to a running `discord-voice-service` endpoint.
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        Ok(Self {
            inner: DiscordVoiceControlClient::connect(dst).await?,
        })
    }

    /// Build a client from an existing Tonic channel.
    pub fn new(channel: Channel) -> Self {
        Self {
            inner: DiscordVoiceControlClient::new(channel),
        }
    }

    /// Wrap an already configured generated Tonic client.
    pub fn from_tonic(inner: DiscordVoiceControlClient<Channel>) -> Self {
        Self { inner }
    }

    /// Borrow the generated Tonic client.
    pub fn tonic(&self) -> &DiscordVoiceControlClient<Channel> {
        &self.inner
    }

    /// Mutably borrow the generated Tonic client.
    pub fn tonic_mut(&mut self) -> &mut DiscordVoiceControlClient<Channel> {
        &mut self.inner
    }

    /// Return the generated Tonic client.
    pub fn into_tonic(self) -> DiscordVoiceControlClient<Channel> {
        self.inner
    }

    /// Forward an authenticated Discord voice context to the service.
    pub async fn join_voice(&mut self, voice: impl Into<VoiceContext>) -> Result<(), Status> {
        self.inner
            .join_voice(JoinVoiceRequest {
                voice: Some(voice.into().into_proto()),
            })
            .await
            .map(|_| ())
    }

    /// Refresh the Discord voice context after Discord rotates session data.
    pub async fn update_voice_context(
        &mut self,
        voice: impl Into<VoiceContext>,
    ) -> Result<(), Status> {
        self.inner
            .update_voice_context(UpdateVoiceContextRequest {
                voice: Some(voice.into().into_proto()),
            })
            .await
            .map(|_| ())
    }

    /// Start playback for a YouTube Music video ID.
    pub async fn play(&mut self, video_id: impl Into<String>) -> Result<(), Status> {
        self.inner
            .play(PlayRequest {
                video_id: video_id.into(),
            })
            .await
            .map(|_| ())
    }

    /// Pause playback.
    pub async fn pause(&mut self) -> Result<(), Status> {
        self.inner.pause(PauseRequest {}).await.map(|_| ())
    }

    /// Resume playback.
    pub async fn resume(&mut self) -> Result<(), Status> {
        self.inner.resume(ResumeRequest {}).await.map(|_| ())
    }

    /// Stop playback while keeping the service voice session available.
    pub async fn stop(&mut self) -> Result<(), Status> {
        self.inner.stop(StopRequest {}).await.map(|_| ())
    }

    /// Leave the active Discord voice session.
    pub async fn leave_voice(&mut self) -> Result<(), Status> {
        self.inner
            .leave_voice(LeaveVoiceRequest {})
            .await
            .map(|_| ())
    }

    /// Fetch the current service session state with Twilight-typed IDs.
    pub async fn state(&mut self) -> Result<StateSnapshot, Status> {
        let snapshot = self.inner.get_state(GetStateRequest {}).await?.into_inner();

        StateSnapshot::try_from(snapshot).map_err(Status::from)
    }

    /// Subscribe to service session events with Twilight-typed IDs.
    pub async fn events(&mut self) -> Result<EventStream, Status> {
        let inner = self
            .inner
            .subscribe_events(SubscribeEventsRequest {})
            .await?
            .into_inner();

        Ok(EventStream { inner })
    }
}

/// Authenticated voice connection data collected from Twilight gateway events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceContext {
    pub guild_id: Id<GuildMarker>,
    pub channel_id: Id<ChannelMarker>,
    pub user_id: Id<UserMarker>,
    pub session_id: String,
    pub endpoint: String,
    pub token: String,
}

impl VoiceContext {
    pub fn new(
        guild_id: Id<GuildMarker>,
        channel_id: Id<ChannelMarker>,
        user_id: Id<UserMarker>,
        session_id: impl Into<String>,
        endpoint: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self {
            guild_id,
            channel_id,
            user_id,
            session_id: session_id.into(),
            endpoint: endpoint.into(),
            token: token.into(),
        }
    }

    /// Convert into the protobuf voice context expected by the service.
    pub fn into_proto(self) -> ProtoVoiceContext {
        ProtoVoiceContext {
            guild_id: self.guild_id.to_string(),
            channel_id: self.channel_id.to_string(),
            user_id: self.user_id.to_string(),
            session_id: self.session_id,
            endpoint: self.endpoint,
            token: self.token,
        }
    }

    /// Clone into the protobuf voice context expected by the service.
    pub fn to_proto(&self) -> ProtoVoiceContext {
        self.clone().into_proto()
    }

    /// Build the Twilight gateway command that joins this voice context's channel.
    pub fn join_command(&self, self_deaf: bool, self_mute: bool) -> UpdateVoiceState {
        join_voice_channel(self.guild_id, self.channel_id, self_deaf, self_mute)
    }
}

impl From<VoiceContext> for ProtoVoiceContext {
    fn from(value: VoiceContext) -> Self {
        value.into_proto()
    }
}

impl From<&VoiceContext> for VoiceContext {
    fn from(value: &VoiceContext) -> Self {
        value.clone()
    }
}

impl From<&VoiceContext> for ProtoVoiceContext {
    fn from(value: &VoiceContext) -> Self {
        value.to_proto()
    }
}

/// Build the Twilight gateway command for joining a guild voice channel.
pub fn join_voice_channel(
    guild_id: Id<GuildMarker>,
    channel_id: Id<ChannelMarker>,
    self_deaf: bool,
    self_mute: bool,
) -> UpdateVoiceState {
    UpdateVoiceState::new(guild_id, Some(channel_id), self_deaf, self_mute)
}

/// Build the Twilight gateway command for leaving a guild voice channel.
pub fn leave_voice_channel(guild_id: Id<GuildMarker>) -> UpdateVoiceState {
    UpdateVoiceState::new(guild_id, None, false, false)
}

/// Tracks Twilight gateway voice events until a complete service voice context is available.
#[derive(Debug, Clone)]
pub struct VoiceContextTracker {
    guild_id: Id<GuildMarker>,
    channel_id: Id<ChannelMarker>,
    user_id: Id<UserMarker>,
    session_id: Option<String>,
    endpoint: Option<String>,
    token: Option<String>,
    current: Option<VoiceContext>,
}

impl VoiceContextTracker {
    /// Create an empty tracker for the bot user and target voice channel.
    pub fn new(
        guild_id: Id<GuildMarker>,
        channel_id: Id<ChannelMarker>,
        user_id: Id<UserMarker>,
    ) -> Self {
        Self {
            guild_id,
            channel_id,
            user_id,
            session_id: None,
            endpoint: None,
            token: None,
            current: None,
        }
    }

    /// Seed a tracker from a context the service is already using.
    pub fn from_context(context: VoiceContext) -> Self {
        Self {
            guild_id: context.guild_id,
            channel_id: context.channel_id,
            user_id: context.user_id,
            session_id: Some(context.session_id.clone()),
            endpoint: Some(context.endpoint.clone()),
            token: Some(context.token.clone()),
            current: Some(context),
        }
    }

    pub fn guild_id(&self) -> Id<GuildMarker> {
        self.guild_id
    }

    pub fn channel_id(&self) -> Id<ChannelMarker> {
        self.channel_id
    }

    pub fn user_id(&self) -> Id<UserMarker> {
        self.user_id
    }

    /// Return the last complete context observed by this tracker.
    pub fn current(&self) -> Option<&VoiceContext> {
        self.current.as_ref()
    }

    /// Observe one Twilight gateway event.
    ///
    /// Returns `Some` only when the event completes the initial context or changes
    /// the context that should be forwarded to `UpdateVoiceContext`.
    pub fn observe(&mut self, event: &Event) -> Option<VoiceContext> {
        match event {
            Event::VoiceStateUpdate(update)
                if update.user_id == self.user_id
                    && update.guild_id == Some(self.guild_id)
                    && update.channel_id == Some(self.channel_id) =>
            {
                self.session_id = Some(update.session_id.clone());
            }
            Event::VoiceServerUpdate(update) if update.guild_id == self.guild_id => {
                if let Some(endpoint) = update.endpoint.as_ref().filter(|value| !value.is_empty()) {
                    self.endpoint = Some(endpoint.clone());
                }

                if !update.token.is_empty() {
                    self.token = Some(update.token.clone());
                }
            }
            _ => {}
        }

        let context = VoiceContext {
            guild_id: self.guild_id,
            channel_id: self.channel_id,
            user_id: self.user_id,
            session_id: self.session_id.as_ref()?.clone(),
            endpoint: self.endpoint.as_ref()?.clone(),
            token: self.token.as_ref()?.clone(),
        };

        if self.current.as_ref() == Some(&context) {
            return None;
        }

        self.current = Some(context.clone());
        Some(context)
    }
}

/// Current service session state with Twilight-typed Discord IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSnapshot {
    pub state: SessionState,
    pub guild_id: Option<Id<GuildMarker>>,
    pub channel_id: Option<Id<ChannelMarker>>,
    pub current_video_id: Option<String>,
    pub queue_depth: u32,
    pub selected_itag: Option<u32>,
    pub message: Option<String>,
}

impl TryFrom<ProtoSessionStateSnapshot> for StateSnapshot {
    type Error = InvalidId;

    fn try_from(value: ProtoSessionStateSnapshot) -> Result<Self, Self::Error> {
        Ok(Self {
            state: SessionState::from_raw(value.state),
            guild_id: parse_optional_id("guild_id", value.guild_id)?,
            channel_id: parse_optional_id("channel_id", value.channel_id)?,
            current_video_id: non_empty(value.current_video_id),
            queue_depth: value.queue_depth,
            selected_itag: non_zero(value.selected_itag),
            message: non_empty(value.message),
        })
    }
}

/// A service event stream that yields Twilight-typed session events.
pub struct EventStream {
    inner: tonic::Streaming<ProtoSessionEvent>,
}

impl EventStream {
    /// Receive the next typed service event.
    pub async fn message(&mut self) -> Result<Option<SessionEvent>, Status> {
        self.inner
            .message()
            .await?
            .map(SessionEvent::try_from)
            .transpose()
            .map_err(Status::from)
    }

    /// Return the raw generated Tonic stream.
    pub fn into_inner(self) -> tonic::Streaming<ProtoSessionEvent> {
        self.inner
    }
}

impl Stream for EventStream {
    type Item = Result<SessionEvent, Status>;

    #[allow(clippy::result_large_err)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_next(cx).map(|item| {
            item.map(|result| match result {
                Ok(event) => SessionEvent::try_from(event).map_err(Status::from),
                Err(status) => Err(status),
            })
        })
    }
}

/// A service session event with Twilight-typed Discord IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEvent {
    pub kind: SessionEventKind,
    pub guild_id: Option<Id<GuildMarker>>,
    pub channel_id: Option<Id<ChannelMarker>>,
    pub current_video_id: Option<String>,
    pub selected_itag: Option<u32>,
    pub message: Option<String>,
    pub error_code: Option<String>,
    pub reason: SessionEventReason,
}

impl Default for SessionEvent {
    fn default() -> Self {
        Self {
            kind: SessionEventKind::Unspecified,
            guild_id: None,
            channel_id: None,
            current_video_id: None,
            selected_itag: None,
            message: None,
            error_code: None,
            reason: SessionEventReason::Unspecified,
        }
    }
}

impl TryFrom<ProtoSessionEvent> for SessionEvent {
    type Error = InvalidId;

    fn try_from(value: ProtoSessionEvent) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: SessionEventKind::from_raw(value.kind),
            guild_id: parse_optional_id("guild_id", value.guild_id)?,
            channel_id: parse_optional_id("channel_id", value.channel_id)?,
            current_video_id: non_empty(value.current_video_id),
            selected_itag: non_zero(value.selected_itag),
            message: non_empty(value.message),
            error_code: non_empty(value.error_code),
            reason: SessionEventReason::from_raw(value.reason),
        })
    }
}

/// Service session states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionState {
    Unspecified,
    Idle,
    ConnectingVoice,
    VoiceReady,
    ResolvingTrack,
    Buffering,
    Playing,
    Paused,
    Stopping,
    Error,
    Unknown(i32),
}

impl SessionState {
    pub fn from_raw(value: i32) -> Self {
        match ProtoSessionState::try_from(value).ok() {
            Some(ProtoSessionState::Unspecified) => Self::Unspecified,
            Some(ProtoSessionState::Idle) => Self::Idle,
            Some(ProtoSessionState::ConnectingVoice) => Self::ConnectingVoice,
            Some(ProtoSessionState::VoiceReadyState) => Self::VoiceReady,
            Some(ProtoSessionState::ResolvingTrack) => Self::ResolvingTrack,
            Some(ProtoSessionState::BufferingState) => Self::Buffering,
            Some(ProtoSessionState::PlayingState) => Self::Playing,
            Some(ProtoSessionState::PausedState) => Self::Paused,
            Some(ProtoSessionState::Stopping) => Self::Stopping,
            Some(ProtoSessionState::ErrorState) => Self::Error,
            None => Self::Unknown(value),
        }
    }
}

/// Service session event kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionEventKind {
    Unspecified,
    VoiceConnecting,
    VoiceReady,
    TrackResolving,
    Buffering,
    Playing,
    Paused,
    Stopped,
    TrackEnded,
    PlaybackInterrupted,
    RecoverableWarning,
    FatalError,
    VoiceReconnecting,
    Unknown(i32),
}

impl SessionEventKind {
    pub fn from_raw(value: i32) -> Self {
        match ProtoSessionEventKind::try_from(value).ok() {
            Some(ProtoSessionEventKind::Unspecified) => Self::Unspecified,
            Some(ProtoSessionEventKind::VoiceConnecting) => Self::VoiceConnecting,
            Some(ProtoSessionEventKind::VoiceReady) => Self::VoiceReady,
            Some(ProtoSessionEventKind::TrackResolving) => Self::TrackResolving,
            Some(ProtoSessionEventKind::Buffering) => Self::Buffering,
            Some(ProtoSessionEventKind::Playing) => Self::Playing,
            Some(ProtoSessionEventKind::Paused) => Self::Paused,
            Some(ProtoSessionEventKind::Stopped) => Self::Stopped,
            Some(ProtoSessionEventKind::TrackEnded) => Self::TrackEnded,
            Some(ProtoSessionEventKind::PlaybackInterrupted) => Self::PlaybackInterrupted,
            Some(ProtoSessionEventKind::RecoverableWarning) => Self::RecoverableWarning,
            Some(ProtoSessionEventKind::FatalError) => Self::FatalError,
            Some(ProtoSessionEventKind::VoiceReconnecting) => Self::VoiceReconnecting,
            None => Self::Unknown(value),
        }
    }

    pub fn as_str_name(self) -> &'static str {
        match self {
            Self::Unspecified => "SESSION_EVENT_KIND_UNSPECIFIED",
            Self::VoiceConnecting => "VOICE_CONNECTING",
            Self::VoiceReady => "VOICE_READY",
            Self::TrackResolving => "TRACK_RESOLVING",
            Self::Buffering => "BUFFERING",
            Self::Playing => "PLAYING",
            Self::Paused => "PAUSED",
            Self::Stopped => "STOPPED",
            Self::TrackEnded => "TRACK_ENDED",
            Self::PlaybackInterrupted => "PLAYBACK_INTERRUPTED",
            Self::RecoverableWarning => "RECOVERABLE_WARNING",
            Self::FatalError => "FATAL_ERROR",
            Self::VoiceReconnecting => "VOICE_RECONNECTING",
            Self::Unknown(_) => "UNKNOWN",
        }
    }
}

/// Service session event reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionEventReason {
    Unspecified,
    JoinTimeout,
    JoinFailed,
    InvalidVoiceToken,
    VoiceResumeFailed,
    DaveTransitionFailed,
    UnsupportedEncryptionMode,
    UdpDiscoveryFailed,
    UpstreamUrlStale,
    PlaybackSourceUnsupported,
    Unknown(i32),
}

impl SessionEventReason {
    pub fn from_raw(value: i32) -> Self {
        match ProtoSessionEventReason::try_from(value).ok() {
            Some(ProtoSessionEventReason::Unspecified) => Self::Unspecified,
            Some(ProtoSessionEventReason::JoinTimeout) => Self::JoinTimeout,
            Some(ProtoSessionEventReason::JoinFailed) => Self::JoinFailed,
            Some(ProtoSessionEventReason::InvalidVoiceToken) => Self::InvalidVoiceToken,
            Some(ProtoSessionEventReason::VoiceResumeFailed) => Self::VoiceResumeFailed,
            Some(ProtoSessionEventReason::DaveTransitionFailed) => Self::DaveTransitionFailed,
            Some(ProtoSessionEventReason::UnsupportedEncryptionMode) => {
                Self::UnsupportedEncryptionMode
            }
            Some(ProtoSessionEventReason::UdpDiscoveryFailed) => Self::UdpDiscoveryFailed,
            Some(ProtoSessionEventReason::UpstreamUrlStale) => Self::UpstreamUrlStale,
            Some(ProtoSessionEventReason::PlaybackSourceUnsupported) => {
                Self::PlaybackSourceUnsupported
            }
            None => Self::Unknown(value),
        }
    }
}

/// Invalid Discord ID data returned from the service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidId {
    field: &'static str,
    value: String,
}

impl InvalidId {
    pub fn field(&self) -> &'static str {
        self.field
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for InvalidId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid Discord snowflake in {}: {:?}",
            self.field, self.value
        )
    }
}

impl std::error::Error for InvalidId {}

impl From<InvalidId> for Status {
    fn from(value: InvalidId) -> Self {
        Status::new(Code::Internal, value.to_string())
    }
}

fn parse_optional_id<T>(field: &'static str, value: String) -> Result<Option<Id<T>>, InvalidId> {
    if value.is_empty() {
        return Ok(None);
    }

    value
        .parse()
        .map(Some)
        .map_err(|_| InvalidId { field, value })
}

fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn non_zero(value: u32) -> Option<u32> {
    (value != 0).then_some(value)
}
