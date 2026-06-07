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
    error::Error as StdError,
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use tonic::{
    Code, Status,
    transport::{Channel, Endpoint},
};
use twilight_gateway::{Event, MessageSender, error::ChannelError};
use twilight_model::{
    gateway::payload::outgoing::UpdateVoiceState,
    id::{
        Id,
        marker::{ChannelMarker, GuildMarker, UserMarker},
    },
};

use crate::proto::{
    DurationStatsSnapshot as ProtoDurationStatsSnapshot, GetPlaybackMetricsRequest,
    GetStateRequest, JoinVoiceRequest, LeaveVoiceRequest, PauseRequest, PlayRequest,
    PlaybackBufferDepthSnapshot as ProtoPlaybackBufferDepthSnapshot,
    PlaybackQueueDepthStatsSnapshot as ProtoPlaybackQueueDepthStatsSnapshot,
    PlaybackSendCommandKind as ProtoPlaybackSendCommandKind,
    PlaybackSendEventSnapshot as ProtoPlaybackSendEventSnapshot,
    PlaybackStabilitySnapshot as ProtoPlaybackStabilitySnapshot,
    PreparedPlayoutQueueEventKind as ProtoPreparedPlayoutQueueEventKind,
    PreparedPlayoutQueueEventReason as ProtoPreparedPlayoutQueueEventReason,
    PreparedPlayoutQueueEventSnapshot as ProtoPreparedPlayoutQueueEventSnapshot,
    PreparedTrackQueueDepthSampleSnapshot as ProtoPreparedTrackQueueDepthSampleSnapshot,
    PreparedTrackQueueSamplePhase as ProtoPreparedTrackQueueSamplePhase, ResumeRequest,
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

/// Error from a playback control RPC paired with a Twilight gateway voice-state command.
#[derive(Debug)]
pub enum GatewayVoiceControlError {
    /// The `discord-voice-service` control RPC failed.
    Control(Status),
    /// Sending the Twilight gateway command failed.
    Gateway(ChannelError),
}

impl fmt::Display for GatewayVoiceControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => write!(f, "voice control RPC failed: {error}"),
            Self::Gateway(error) => write!(f, "gateway voice-state command failed: {error}"),
        }
    }
}

impl StdError for GatewayVoiceControlError {}

impl From<Status> for GatewayVoiceControlError {
    fn from(value: Status) -> Self {
        Self::Control(value)
    }
}

impl From<ChannelError> for GatewayVoiceControlError {
    fn from(value: ChannelError) -> Self {
        Self::Gateway(value)
    }
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

    /// Pause playback and mark the bot self-muted on the Discord gateway.
    ///
    /// The service stops voice media before the gateway mute command is queued, so a successful
    /// call gives the observer-visible speaking state a Discord voice-state transition while the
    /// service media sender is already paused.
    pub async fn pause_and_self_mute(
        &mut self,
        sender: &MessageSender,
        voice: &VoiceContext,
        self_deaf: bool,
    ) -> Result<(), GatewayVoiceControlError> {
        self.pause().await?;
        set_voice_self_mute(sender, voice, self_deaf, true)?;
        Ok(())
    }

    /// Pause playback and leave the Discord voice channel.
    ///
    /// The service suspends voice media before the gateway leave command is queued, so observers
    /// can see the bot disappear from voice while no service RTP is being sent.
    pub async fn pause_and_leave(
        &mut self,
        sender: &MessageSender,
        voice: &VoiceContext,
    ) -> Result<(), GatewayVoiceControlError> {
        self.pause().await?;
        sender.command(&leave_voice_channel(voice.guild_id))?;
        Ok(())
    }

    /// Resume playback.
    pub async fn resume(&mut self) -> Result<(), Status> {
        self.inner.resume(ResumeRequest {}).await.map(|_| ())
    }

    /// Mark the bot self-unmuted on the Discord gateway and resume playback.
    ///
    /// The gateway unmute is queued before playback resumes so observers can receive resumed
    /// service packets after the visible speaking state returns.
    pub async fn resume_and_self_unmute(
        &mut self,
        sender: &MessageSender,
        voice: &VoiceContext,
        self_deaf: bool,
    ) -> Result<(), GatewayVoiceControlError> {
        set_voice_self_mute(sender, voice, self_deaf, false)?;
        self.resume().await?;
        Ok(())
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

    /// Fetch the latest playback stability metrics snapshot.
    pub async fn playback_metrics(&mut self) -> Result<PlaybackStabilitySnapshot, Status> {
        let snapshot = self
            .inner
            .get_playback_metrics(GetPlaybackMetricsRequest {})
            .await?
            .into_inner();

        Ok(snapshot.into())
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

/// Queue a Twilight gateway voice-state update that keeps the current channel and toggles self-mute.
pub fn set_voice_self_mute(
    sender: &MessageSender,
    voice: &VoiceContext,
    self_deaf: bool,
    self_mute: bool,
) -> Result<(), ChannelError> {
    sender.command(&voice.join_command(self_deaf, self_mute))
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

    /// Clear the tracked context after the bot leaves the target voice channel.
    pub fn reset(&mut self) {
        self.session_id = None;
        self.endpoint = None;
        self.token = None;
        self.current = None;
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
                    && update.channel_id.is_none() =>
            {
                self.reset();
            }
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

/// Duration percentile statistics captured by the service.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DurationStatsSnapshot {
    pub samples: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub min_ms: u64,
    pub max_ms: u64,
}

impl From<ProtoDurationStatsSnapshot> for DurationStatsSnapshot {
    fn from(value: ProtoDurationStatsSnapshot) -> Self {
        Self {
            samples: value.samples,
            p50_ms: value.p50_ms,
            p95_ms: value.p95_ms,
            p99_ms: value.p99_ms,
            min_ms: value.min_ms,
            max_ms: value.max_ms,
        }
    }
}

/// Compressed Opus buffer depth captured by the service.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaybackBufferDepthSnapshot {
    pub packets: u64,
    pub bytes: u64,
    pub duration_ms: u64,
    pub duration_samples: u64,
}

impl From<ProtoPlaybackBufferDepthSnapshot> for PlaybackBufferDepthSnapshot {
    fn from(value: ProtoPlaybackBufferDepthSnapshot) -> Self {
        Self {
            packets: value.packets,
            bytes: value.bytes,
            duration_ms: value.duration_ms,
            duration_samples: value.duration_samples,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaybackQueueDepthStatsSnapshot {
    pub sample_count: u64,
    pub empty_count: u64,
    pub current_depth: PlaybackBufferDepthSnapshot,
    pub min_depth: PlaybackBufferDepthSnapshot,
    pub p5_depth: PlaybackBufferDepthSnapshot,
    pub p50_depth: PlaybackBufferDepthSnapshot,
    pub p95_depth: PlaybackBufferDepthSnapshot,
    pub max_depth: PlaybackBufferDepthSnapshot,
}

impl From<ProtoPlaybackQueueDepthStatsSnapshot> for PlaybackQueueDepthStatsSnapshot {
    fn from(value: ProtoPlaybackQueueDepthStatsSnapshot) -> Self {
        Self {
            sample_count: value.sample_count,
            empty_count: value.empty_count,
            current_depth: value.current_depth.unwrap_or_default().into(),
            min_depth: value.min_depth.unwrap_or_default().into(),
            p5_depth: value.p5_depth.unwrap_or_default().into(),
            p50_depth: value.p50_depth.unwrap_or_default().into(),
            p95_depth: value.p95_depth.unwrap_or_default().into(),
            max_depth: value.max_depth.unwrap_or_default().into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaybackSendCommandKind {
    Track,
    ScheduledSilence,
    BoundarySilence,
    OtherBoundary,
    Unspecified,
}

impl Default for PlaybackSendCommandKind {
    fn default() -> Self {
        Self::Unspecified
    }
}

impl PlaybackSendCommandKind {
    pub fn from_raw(value: i32) -> Self {
        match ProtoPlaybackSendCommandKind::try_from(value) {
            Ok(ProtoPlaybackSendCommandKind::Track) => Self::Track,
            Ok(ProtoPlaybackSendCommandKind::ScheduledSilence) => Self::ScheduledSilence,
            Ok(ProtoPlaybackSendCommandKind::BoundarySilence) => Self::BoundarySilence,
            Ok(ProtoPlaybackSendCommandKind::OtherBoundary) => Self::OtherBoundary,
            Ok(ProtoPlaybackSendCommandKind::Unspecified) | Err(_) => Self::Unspecified,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedTrackQueueSamplePhase {
    ActivePrePause,
    ActivePostResume,
    Unspecified,
}

impl Default for PreparedTrackQueueSamplePhase {
    fn default() -> Self {
        Self::Unspecified
    }
}

impl PreparedTrackQueueSamplePhase {
    pub fn from_raw(value: i32) -> Self {
        match ProtoPreparedTrackQueueSamplePhase::try_from(value) {
            Ok(ProtoPreparedTrackQueueSamplePhase::ActivePrePause) => Self::ActivePrePause,
            Ok(ProtoPreparedTrackQueueSamplePhase::ActivePostResume) => Self::ActivePostResume,
            Ok(ProtoPreparedTrackQueueSamplePhase::Unspecified) | Err(_) => Self::Unspecified,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedPlayoutQueueEventKind {
    Enqueued,
    DequeuedToDeadlineSender,
    DroppedBeforeSend,
    Rebuilt,
    Unspecified,
}

impl Default for PreparedPlayoutQueueEventKind {
    fn default() -> Self {
        Self::Unspecified
    }
}

impl PreparedPlayoutQueueEventKind {
    pub fn from_raw(value: i32) -> Self {
        match ProtoPreparedPlayoutQueueEventKind::try_from(value) {
            Ok(ProtoPreparedPlayoutQueueEventKind::Enqueued) => Self::Enqueued,
            Ok(ProtoPreparedPlayoutQueueEventKind::DequeuedToDeadlineSender) => {
                Self::DequeuedToDeadlineSender
            }
            Ok(ProtoPreparedPlayoutQueueEventKind::DroppedBeforeSend) => Self::DroppedBeforeSend,
            Ok(ProtoPreparedPlayoutQueueEventKind::Rebuilt) => Self::Rebuilt,
            Ok(ProtoPreparedPlayoutQueueEventKind::Unspecified) | Err(_) => Self::Unspecified,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedPlayoutQueueEventReason {
    SteadyPlayback,
    Pause,
    Stop,
    DaveTransitionRecovery,
    Reconnect,
    SourceUnderrun,
    NaturalEnd,
    Interruption,
    Unspecified,
}

impl Default for PreparedPlayoutQueueEventReason {
    fn default() -> Self {
        Self::Unspecified
    }
}

impl PreparedPlayoutQueueEventReason {
    pub fn from_raw(value: i32) -> Self {
        match ProtoPreparedPlayoutQueueEventReason::try_from(value) {
            Ok(ProtoPreparedPlayoutQueueEventReason::SteadyPlayback) => Self::SteadyPlayback,
            Ok(ProtoPreparedPlayoutQueueEventReason::Pause) => Self::Pause,
            Ok(ProtoPreparedPlayoutQueueEventReason::Stop) => Self::Stop,
            Ok(ProtoPreparedPlayoutQueueEventReason::DaveTransitionRecovery) => {
                Self::DaveTransitionRecovery
            }
            Ok(ProtoPreparedPlayoutQueueEventReason::Reconnect) => Self::Reconnect,
            Ok(ProtoPreparedPlayoutQueueEventReason::SourceUnderrun) => Self::SourceUnderrun,
            Ok(ProtoPreparedPlayoutQueueEventReason::NaturalEnd) => Self::NaturalEnd,
            Ok(ProtoPreparedPlayoutQueueEventReason::Interruption) => Self::Interruption,
            Ok(ProtoPreparedPlayoutQueueEventReason::Unspecified) | Err(_) => Self::Unspecified,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaybackSendEventSnapshot {
    pub packet_index: u64,
    pub command_kind: PlaybackSendCommandKind,
    pub expected_deadline_offset_us: u64,
    pub send_started_offset_us: u64,
    pub sent_offset_us: u64,
    pub media_duration_ms: u64,
    pub media_duration_samples: u32,
    pub rtp_sequence: u32,
    pub rtp_timestamp: u32,
    pub protection_nonce: Option<u32>,
    pub source_frame_epoch: Option<u64>,
    pub source_media_position_ms: Option<u64>,
    pub source_media_byte_position: Option<u64>,
    pub committed_heard_media: bool,
}

impl From<ProtoPlaybackSendEventSnapshot> for PlaybackSendEventSnapshot {
    fn from(value: ProtoPlaybackSendEventSnapshot) -> Self {
        Self {
            packet_index: value.packet_index,
            command_kind: PlaybackSendCommandKind::from_raw(value.command_kind),
            expected_deadline_offset_us: value.expected_deadline_offset_us,
            send_started_offset_us: value.send_started_offset_us,
            sent_offset_us: value.sent_offset_us,
            media_duration_ms: value.media_duration_ms,
            media_duration_samples: value.media_duration_samples,
            rtp_sequence: value.rtp_sequence,
            rtp_timestamp: value.rtp_timestamp,
            protection_nonce: value.protection_nonce,
            source_frame_epoch: value.source_frame_epoch,
            source_media_position_ms: value.source_media_position_ms,
            source_media_byte_position: value.source_media_byte_position,
            committed_heard_media: value.committed_heard_media,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreparedTrackQueueDepthSampleSnapshot {
    pub sample_index: u64,
    pub phase: PreparedTrackQueueSamplePhase,
    pub depth: PlaybackBufferDepthSnapshot,
}

impl From<ProtoPreparedTrackQueueDepthSampleSnapshot> for PreparedTrackQueueDepthSampleSnapshot {
    fn from(value: ProtoPreparedTrackQueueDepthSampleSnapshot) -> Self {
        Self {
            sample_index: value.sample_index,
            phase: PreparedTrackQueueSamplePhase::from_raw(value.phase),
            depth: value.depth.unwrap_or_default().into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreparedPlayoutQueueEventSnapshot {
    pub event_index: u64,
    pub event_kind: PreparedPlayoutQueueEventKind,
    pub reason: PreparedPlayoutQueueEventReason,
    pub command_kind: PlaybackSendCommandKind,
    pub media_duration_ms: u64,
    pub media_duration_samples: u32,
    pub rtp_sequence: u32,
    pub rtp_timestamp: u32,
    pub protection_nonce: Option<u32>,
    pub source_frame_epoch: Option<u64>,
    pub source_media_position_ms: Option<u64>,
    pub source_media_byte_position: Option<u64>,
    pub queue_depth_after: PlaybackBufferDepthSnapshot,
}

impl From<ProtoPreparedPlayoutQueueEventSnapshot> for PreparedPlayoutQueueEventSnapshot {
    fn from(value: ProtoPreparedPlayoutQueueEventSnapshot) -> Self {
        Self {
            event_index: value.event_index,
            event_kind: PreparedPlayoutQueueEventKind::from_raw(value.event_kind),
            reason: PreparedPlayoutQueueEventReason::from_raw(value.reason),
            command_kind: PlaybackSendCommandKind::from_raw(value.command_kind),
            media_duration_ms: value.media_duration_ms,
            media_duration_samples: value.media_duration_samples,
            rtp_sequence: value.rtp_sequence,
            rtp_timestamp: value.rtp_timestamp,
            protection_nonce: value.protection_nonce,
            source_frame_epoch: value.source_frame_epoch,
            source_media_position_ms: value.source_media_position_ms,
            source_media_byte_position: value.source_media_byte_position,
            queue_depth_after: value.queue_depth_after.unwrap_or_default().into(),
        }
    }
}

/// Latest structured stability metrics for a playback epoch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaybackStabilitySnapshot {
    pub available: bool,
    pub playback_epoch: u64,
    pub video_id: Option<String>,
    pub selected_itag: Option<u32>,
    pub track_packet_count: u64,
    pub continuity_silence_packet_count: u64,
    pub inserted_silence_duration_ms: u64,
    pub track_interval: DurationStatsSnapshot,
    pub track_media_duration_sent_ms: u64,
    pub track_wall_clock_elapsed_ms: u64,
    pub track_media_to_wall_clock_ratio_ppm: u64,
    pub track_fast_interval_count: u64,
    pub track_fast_interval_min_ms: u64,
    pub track_fast_interval_min_us: u64,
    pub track_tempo_window_count: u64,
    pub track_tempo_window_post_source_buffer_count: u64,
    pub track_tempo_window_min_ratio_ppm: u64,
    pub track_tempo_window_max_ratio_ppm: u64,
    pub track_tempo_window_fast_count: u64,
    pub track_tempo_window_fastest_ratio_ppm: u64,
    pub track_tempo_window_fastest_media_ms: u64,
    pub track_tempo_window_fastest_wall_clock_us: u64,
    pub track_tempo_window_slow_count: u64,
    pub track_tempo_window_slowest_ratio_ppm: u64,
    pub track_tempo_window_slowest_media_ms: u64,
    pub track_tempo_window_slowest_wall_clock_us: u64,
    pub skipped_source_frame_count: u64,
    pub skipped_source_duration_ms: u64,
    pub tempo_rebase_count: u64,
    pub all_packet_interval: DurationStatsSnapshot,
    pub sender_lateness: DurationStatsSnapshot,
    pub max_consecutive_late_packets: u64,
    pub current_consecutive_late_packets: u64,
    pub current_buffer_depth: PlaybackBufferDepthSnapshot,
    pub min_buffer_depth: PlaybackBufferDepthSnapshot,
    pub max_buffer_depth: PlaybackBufferDepthSnapshot,
    pub current_source_buffer_depth: PlaybackBufferDepthSnapshot,
    pub min_source_buffer_depth: PlaybackBufferDepthSnapshot,
    pub max_source_buffer_depth: PlaybackBufferDepthSnapshot,
    pub source_buffer_depth: Option<PlaybackQueueDepthStatsSnapshot>,
    pub current_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    pub min_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    pub max_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    pub egress_buffer_target_ms: u64,
    pub current_egress_buffer_depth: PlaybackBufferDepthSnapshot,
    pub min_egress_buffer_depth: PlaybackBufferDepthSnapshot,
    pub max_egress_buffer_depth: PlaybackBufferDepthSnapshot,
    pub prepared_rtp_queue_depth_ms: u64,
    pub prepared_track_queue_target_ms: u64,
    pub prepared_track_queue_low_watermark_ms: u64,
    pub prepared_track_queue_high_watermark_ms: u64,
    pub active_pre_pause_prepared_track_queue_depth: Option<PlaybackQueueDepthStatsSnapshot>,
    pub active_post_resume_prepared_track_queue_depth: Option<PlaybackQueueDepthStatsSnapshot>,
    pub prepared_track_queue_depth_sample_count: u64,
    pub prepared_track_queue_empty_count: u64,
    pub raw_send_events: Vec<PlaybackSendEventSnapshot>,
    pub raw_prepared_track_queue_samples: Vec<PreparedTrackQueueDepthSampleSnapshot>,
    pub raw_prepared_playout_queue_events: Vec<PreparedPlayoutQueueEventSnapshot>,
    pub current_scheduled_silence_queue_depth: PlaybackBufferDepthSnapshot,
    pub max_scheduled_silence_queue_depth: PlaybackBufferDepthSnapshot,
    pub current_boundary_queue_depth: PlaybackBufferDepthSnapshot,
    pub max_boundary_queue_depth: PlaybackBufferDepthSnapshot,
    pub prepared_track_packet_drop_count: u64,
    pub prepared_silence_packet_drop_count: u64,
    pub prepared_packet_rebuild_count: u64,
    pub scheduled_silence_packet_count: u64,
    pub pause_media_boundary_count: u64,
    pub stop_media_boundary_count: u64,
    pub recovery_media_boundary_count: u64,
    pub natural_end_media_boundary_count: u64,
    pub dave_transition_recovery_reached_builder_count: u64,
    pub dave_transition_recovery_reached_deadline_sender_count: u64,
    pub source_underrun_reached_builder_count: u64,
    pub source_underrun_reached_deadline_sender_count: u64,
    pub discarded_source_frame_count: u64,
    pub discarded_source_duration_ms: u64,
    pub stop_discarded_source_frame_count: u64,
    pub stop_discarded_source_duration_ms: u64,
    pub interruption_discarded_source_frame_count: u64,
    pub interruption_discarded_source_duration_ms: u64,
    pub restored_source_frame_count: u64,
    pub restored_source_duration_ms: u64,
    pub source_buffer_target_ms: u64,
    pub adaptive_buffer_target_ms: u64,
    pub max_adaptive_buffer_target_ms: u64,
    pub buffer_low_watermark_count: u64,
    pub source_buffer_low_watermark_count: u64,
    pub playout_buffer_low_watermark_count: u64,
    pub buffer_underrun_count: u64,
    pub playout_underrun_count: u64,
    pub egress_underrun_count: u64,
    pub egress_inserted_silence_duration_ms: u64,
    pub egress_dropped_music_frame_count: u64,
    pub egress_dropped_music_duration_ms: u64,
    pub source_underrun_count: u64,
    pub rebuffer_count: u64,
    pub refill_duration: DurationStatsSnapshot,
    pub source_producer_fill_duration: DurationStatsSnapshot,
    pub producer_stall_duration: DurationStatsSnapshot,
    pub max_producer_lag_ms: u64,
    pub http_retry_count: u64,
    pub response_open_count: u64,
    pub range_reopen_count: u64,
    pub read_error_reopen_count: u64,
    pub url_reresolve_count: u64,
    pub pause_resume_first_intervals_ms: Vec<u64>,
    pub post_stall_first_intervals_ms: Vec<u64>,
    pub post_rebuffer_first_intervals_ms: Vec<u64>,
    pub playout_sender_lateness: DurationStatsSnapshot,
    pub playout_builder_prepare_duration: DurationStatsSnapshot,
    pub sender_send_duration: DurationStatsSnapshot,
    pub sender_loop_non_send_work_duration: DurationStatsSnapshot,
    pub max_consecutive_playout_late_packets: u64,
    pub max_consecutive_late_egress_ticks: u64,
    pub speaking_prepare_duration: DurationStatsSnapshot,
    pub sender_forbidden_work_count: u64,
    pub gateway_event_drain_duration: DurationStatsSnapshot,
    pub gateway_event_drain_count: u64,
    pub dave_transition_count: u64,
    pub dave_transition_count_during_playback: u64,
    pub stale_dave_send_prevented_count: u64,
    pub controlled_media_interruption_count: u64,
    pub media_clock_reset_count: u64,
    pub egress_clock_reset_count: u64,
    pub scheduler_late_reset_count: u64,
    pub source_underrun_reset_count: u64,
    pub pause_resume_reset_count: u64,
    pub dave_transition_recovery_reset_count: u64,
    pub gateway_interruptions: u64,
    pub dave_interruptions: u64,
    pub reconnect_interruptions: u64,
    pub ended: bool,
}

impl From<ProtoPlaybackStabilitySnapshot> for PlaybackStabilitySnapshot {
    fn from(value: ProtoPlaybackStabilitySnapshot) -> Self {
        Self {
            available: value.available,
            playback_epoch: value.playback_epoch,
            video_id: non_empty(value.video_id),
            selected_itag: non_zero(value.selected_itag),
            track_packet_count: value.track_packet_count,
            continuity_silence_packet_count: value.continuity_silence_packet_count,
            inserted_silence_duration_ms: value.inserted_silence_duration_ms,
            track_interval: value.track_interval.unwrap_or_default().into(),
            track_media_duration_sent_ms: value.track_media_duration_sent_ms,
            track_wall_clock_elapsed_ms: value.track_wall_clock_elapsed_ms,
            track_media_to_wall_clock_ratio_ppm: value.track_media_to_wall_clock_ratio_ppm,
            track_fast_interval_count: value.track_fast_interval_count,
            track_fast_interval_min_ms: value.track_fast_interval_min_ms,
            track_fast_interval_min_us: value.track_fast_interval_min_us,
            track_tempo_window_count: value.track_tempo_window_count,
            track_tempo_window_post_source_buffer_count: value
                .track_tempo_window_post_source_buffer_count,
            track_tempo_window_min_ratio_ppm: value.track_tempo_window_min_ratio_ppm,
            track_tempo_window_max_ratio_ppm: value.track_tempo_window_max_ratio_ppm,
            track_tempo_window_fast_count: value.track_tempo_window_fast_count,
            track_tempo_window_fastest_ratio_ppm: value.track_tempo_window_fastest_ratio_ppm,
            track_tempo_window_fastest_media_ms: value.track_tempo_window_fastest_media_ms,
            track_tempo_window_fastest_wall_clock_us: value
                .track_tempo_window_fastest_wall_clock_us,
            track_tempo_window_slow_count: value.track_tempo_window_slow_count,
            track_tempo_window_slowest_ratio_ppm: value.track_tempo_window_slowest_ratio_ppm,
            track_tempo_window_slowest_media_ms: value.track_tempo_window_slowest_media_ms,
            track_tempo_window_slowest_wall_clock_us: value
                .track_tempo_window_slowest_wall_clock_us,
            skipped_source_frame_count: value.skipped_source_frame_count,
            skipped_source_duration_ms: value.skipped_source_duration_ms,
            tempo_rebase_count: value.tempo_rebase_count,
            all_packet_interval: value.all_packet_interval.unwrap_or_default().into(),
            sender_lateness: value.sender_lateness.unwrap_or_default().into(),
            max_consecutive_late_packets: value.max_consecutive_late_packets,
            current_consecutive_late_packets: value.current_consecutive_late_packets,
            current_buffer_depth: value.current_buffer_depth.unwrap_or_default().into(),
            min_buffer_depth: value.min_buffer_depth.unwrap_or_default().into(),
            max_buffer_depth: value.max_buffer_depth.unwrap_or_default().into(),
            current_source_buffer_depth: value
                .current_source_buffer_depth
                .unwrap_or_default()
                .into(),
            min_source_buffer_depth: value.min_source_buffer_depth.unwrap_or_default().into(),
            max_source_buffer_depth: value.max_source_buffer_depth.unwrap_or_default().into(),
            source_buffer_depth: value.source_buffer_depth.map(Into::into),
            current_playout_buffer_depth: value
                .current_playout_buffer_depth
                .unwrap_or_default()
                .into(),
            min_playout_buffer_depth: value.min_playout_buffer_depth.unwrap_or_default().into(),
            max_playout_buffer_depth: value.max_playout_buffer_depth.unwrap_or_default().into(),
            egress_buffer_target_ms: value.egress_buffer_target_ms,
            current_egress_buffer_depth: value
                .current_egress_buffer_depth
                .unwrap_or_default()
                .into(),
            min_egress_buffer_depth: value.min_egress_buffer_depth.unwrap_or_default().into(),
            max_egress_buffer_depth: value.max_egress_buffer_depth.unwrap_or_default().into(),
            prepared_rtp_queue_depth_ms: value.prepared_rtp_queue_depth_ms,
            prepared_track_queue_target_ms: value.prepared_track_queue_target_ms,
            prepared_track_queue_low_watermark_ms: value.prepared_track_queue_low_watermark_ms,
            prepared_track_queue_high_watermark_ms: value.prepared_track_queue_high_watermark_ms,
            active_pre_pause_prepared_track_queue_depth: value
                .active_pre_pause_prepared_track_queue_depth
                .map(Into::into),
            active_post_resume_prepared_track_queue_depth: value
                .active_post_resume_prepared_track_queue_depth
                .map(Into::into),
            prepared_track_queue_depth_sample_count: value.prepared_track_queue_depth_sample_count,
            prepared_track_queue_empty_count: value.prepared_track_queue_empty_count,
            raw_send_events: value.raw_send_events.into_iter().map(Into::into).collect(),
            raw_prepared_track_queue_samples: value
                .raw_prepared_track_queue_samples
                .into_iter()
                .map(Into::into)
                .collect(),
            raw_prepared_playout_queue_events: value
                .raw_prepared_playout_queue_events
                .into_iter()
                .map(Into::into)
                .collect(),
            current_scheduled_silence_queue_depth: value
                .current_scheduled_silence_queue_depth
                .unwrap_or_default()
                .into(),
            max_scheduled_silence_queue_depth: value
                .max_scheduled_silence_queue_depth
                .unwrap_or_default()
                .into(),
            current_boundary_queue_depth: value
                .current_boundary_queue_depth
                .unwrap_or_default()
                .into(),
            max_boundary_queue_depth: value.max_boundary_queue_depth.unwrap_or_default().into(),
            prepared_track_packet_drop_count: value.prepared_track_packet_drop_count,
            prepared_silence_packet_drop_count: value.prepared_silence_packet_drop_count,
            prepared_packet_rebuild_count: value.prepared_packet_rebuild_count,
            scheduled_silence_packet_count: value.scheduled_silence_packet_count,
            pause_media_boundary_count: value.pause_media_boundary_count,
            stop_media_boundary_count: value.stop_media_boundary_count,
            recovery_media_boundary_count: value.recovery_media_boundary_count,
            natural_end_media_boundary_count: value.natural_end_media_boundary_count,
            dave_transition_recovery_reached_builder_count: value
                .dave_transition_recovery_reached_builder_count,
            dave_transition_recovery_reached_deadline_sender_count: value
                .dave_transition_recovery_reached_deadline_sender_count,
            source_underrun_reached_builder_count: value.source_underrun_reached_builder_count,
            source_underrun_reached_deadline_sender_count: value
                .source_underrun_reached_deadline_sender_count,
            discarded_source_frame_count: value.discarded_source_frame_count,
            discarded_source_duration_ms: value.discarded_source_duration_ms,
            stop_discarded_source_frame_count: value.stop_discarded_source_frame_count,
            stop_discarded_source_duration_ms: value.stop_discarded_source_duration_ms,
            interruption_discarded_source_frame_count: value
                .interruption_discarded_source_frame_count,
            interruption_discarded_source_duration_ms: value
                .interruption_discarded_source_duration_ms,
            restored_source_frame_count: value.restored_source_frame_count,
            restored_source_duration_ms: value.restored_source_duration_ms,
            source_buffer_target_ms: value.source_buffer_target_ms,
            adaptive_buffer_target_ms: value.adaptive_buffer_target_ms,
            max_adaptive_buffer_target_ms: value.max_adaptive_buffer_target_ms,
            buffer_low_watermark_count: value.buffer_low_watermark_count,
            source_buffer_low_watermark_count: value.source_buffer_low_watermark_count,
            playout_buffer_low_watermark_count: value.playout_buffer_low_watermark_count,
            buffer_underrun_count: value.buffer_underrun_count,
            playout_underrun_count: value.playout_underrun_count,
            egress_underrun_count: value.egress_underrun_count,
            egress_inserted_silence_duration_ms: value.egress_inserted_silence_duration_ms,
            egress_dropped_music_frame_count: value.egress_dropped_music_frame_count,
            egress_dropped_music_duration_ms: value.egress_dropped_music_duration_ms,
            source_underrun_count: value.source_underrun_count,
            rebuffer_count: value.rebuffer_count,
            refill_duration: value.refill_duration.unwrap_or_default().into(),
            source_producer_fill_duration: value
                .source_producer_fill_duration
                .unwrap_or_default()
                .into(),
            producer_stall_duration: value.producer_stall_duration.unwrap_or_default().into(),
            max_producer_lag_ms: value.max_producer_lag_ms,
            http_retry_count: value.http_retry_count,
            response_open_count: value.response_open_count,
            range_reopen_count: value.range_reopen_count,
            read_error_reopen_count: value.read_error_reopen_count,
            url_reresolve_count: value.url_reresolve_count,
            pause_resume_first_intervals_ms: value.pause_resume_first_intervals_ms,
            post_stall_first_intervals_ms: value.post_stall_first_intervals_ms,
            post_rebuffer_first_intervals_ms: value.post_rebuffer_first_intervals_ms,
            playout_sender_lateness: value.playout_sender_lateness.unwrap_or_default().into(),
            playout_builder_prepare_duration: value
                .playout_builder_prepare_duration
                .unwrap_or_default()
                .into(),
            sender_send_duration: value.sender_send_duration.unwrap_or_default().into(),
            sender_loop_non_send_work_duration: value
                .sender_loop_non_send_work_duration
                .unwrap_or_default()
                .into(),
            max_consecutive_playout_late_packets: value.max_consecutive_playout_late_packets,
            max_consecutive_late_egress_ticks: value.max_consecutive_late_egress_ticks,
            speaking_prepare_duration: value.speaking_prepare_duration.unwrap_or_default().into(),
            sender_forbidden_work_count: value.sender_forbidden_work_count,
            gateway_event_drain_duration: value
                .gateway_event_drain_duration
                .unwrap_or_default()
                .into(),
            gateway_event_drain_count: value.gateway_event_drain_count,
            dave_transition_count: value.dave_transition_count,
            dave_transition_count_during_playback: value.dave_transition_count_during_playback,
            stale_dave_send_prevented_count: value.stale_dave_send_prevented_count,
            controlled_media_interruption_count: value.controlled_media_interruption_count,
            media_clock_reset_count: value.media_clock_reset_count,
            egress_clock_reset_count: value.egress_clock_reset_count,
            scheduler_late_reset_count: value.scheduler_late_reset_count,
            source_underrun_reset_count: value.source_underrun_reset_count,
            pause_resume_reset_count: value.pause_resume_reset_count,
            dave_transition_recovery_reset_count: value.dave_transition_recovery_reset_count,
            gateway_interruptions: value.gateway_interruptions,
            dave_interruptions: value.dave_interruptions,
            reconnect_interruptions: value.reconnect_interruptions,
            ended: value.ended,
        }
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
