use std::pin::Pin;
use std::sync::Arc;

use discord_voice_service_proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControl;
use discord_voice_service_proto::discordvoice::v1::join_voice_request;
use discord_voice_service_proto::discordvoice::v1::{
    GetStateRequest, JoinVoiceRequest, JoinVoiceResponse, LeaveVoiceRequest, LeaveVoiceResponse,
    PauseRequest, PauseResponse, PlayRequest, PlayResponse, ResumeRequest, ResumeResponse,
    SessionEvent, SessionState as ProtoSessionState, SessionStateSnapshot, StopRequest,
    StopResponse, SubscribeEventsRequest, UpdateVoiceContextRequest, UpdateVoiceContextResponse,
};
use discord_voice_service_voice::VoiceContext;
use futures::{Stream, stream};
use tonic::{Request, Response, Status};

use crate::session::state::SessionState;
use crate::session::supervisor::{Command, Supervisor};
use crate::{observability, session::readiness::Readiness};

pub fn map_play_request(request: PlayRequest) -> String {
    request.video_id
}

pub struct ControlService {
    pub supervisor: Supervisor,
    pub readiness: Arc<Readiness>,
}

#[tonic::async_trait]
impl DiscordVoiceControl for ControlService {
    type SubscribeEventsStream =
        Pin<Box<dyn Stream<Item = Result<SessionEvent, Status>> + Send + 'static>>;

    async fn join_voice(
        &self,
        request: Request<JoinVoiceRequest>,
    ) -> Result<Response<JoinVoiceResponse>, Status> {
        let voice = request
            .into_inner()
            .voice
            .ok_or_else(|| Status::invalid_argument("missing voice context"))
            .and_then(map_voice_context);
        let voice = observe_early_status("join_voice", voice)?;

        let result = self
            .supervisor
            .send(Command::JoinVoice { voice })
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("join_voice", &result);
        result?;

        Ok(Response::new(JoinVoiceResponse {}))
    }

    async fn update_voice_context(
        &self,
        request: Request<UpdateVoiceContextRequest>,
    ) -> Result<Response<UpdateVoiceContextResponse>, Status> {
        let voice = request
            .into_inner()
            .voice
            .ok_or_else(|| Status::invalid_argument("missing voice context"))
            .and_then(map_voice_context);
        let voice = observe_early_status("update_voice_context", voice)?;

        let result = self
            .supervisor
            .send(Command::UpdateVoiceContext { voice })
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("update_voice_context", &result);
        result?;
        Ok(Response::new(UpdateVoiceContextResponse {}))
    }

    async fn play(&self, request: Request<PlayRequest>) -> Result<Response<PlayResponse>, Status> {
        let request = request.into_inner();
        observe_early_status("play", validate_non_empty("video_id", &request.video_id))?;

        let result = self
            .supervisor
            .send(Command::Play {
                video_id: request.video_id,
            })
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("play", &result);
        result?;

        Ok(Response::new(PlayResponse {}))
    }

    async fn pause(
        &self,
        _request: Request<PauseRequest>,
    ) -> Result<Response<PauseResponse>, Status> {
        let result = self
            .supervisor
            .send(Command::Pause)
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("pause", &result);
        result?;
        Ok(Response::new(PauseResponse {}))
    }

    async fn resume(
        &self,
        _request: Request<ResumeRequest>,
    ) -> Result<Response<ResumeResponse>, Status> {
        let result = self
            .supervisor
            .send(Command::Resume)
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("resume", &result);
        result?;
        Ok(Response::new(ResumeResponse {}))
    }

    async fn stop(&self, _request: Request<StopRequest>) -> Result<Response<StopResponse>, Status> {
        let result = self
            .supervisor
            .send(Command::Stop)
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("stop", &result);
        result?;
        Ok(Response::new(StopResponse {}))
    }

    async fn leave_voice(
        &self,
        _request: Request<LeaveVoiceRequest>,
    ) -> Result<Response<LeaveVoiceResponse>, Status> {
        let result = self
            .supervisor
            .send(Command::LeaveVoice)
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("leave_voice", &result);
        result?;
        Ok(Response::new(LeaveVoiceResponse {}))
    }

    async fn get_state(
        &self,
        _request: Request<GetStateRequest>,
    ) -> Result<Response<SessionStateSnapshot>, Status> {
        let snapshot = self.supervisor.snapshot().await;
        let readiness = self.readiness.snapshot().await;
        observability::global().record_state_query(&snapshot, readiness);
        observability::global().record_rpc("get_state", tonic::Code::Ok);
        Ok(Response::new(SessionStateSnapshot {
            state: map_session_state(snapshot.state) as i32,
            guild_id: snapshot.guild_id.unwrap_or_default(),
            channel_id: snapshot.channel_id.unwrap_or_default(),
            current_video_id: snapshot.current_video_id.unwrap_or_default(),
            queue_depth: u32::try_from(snapshot.queue_depth).unwrap_or(u32::MAX),
            selected_itag: snapshot.selected_itag.unwrap_or_default(),
            message: snapshot.last_reason.unwrap_or_default(),
        }))
    }

    async fn subscribe_events(
        &self,
        _request: Request<SubscribeEventsRequest>,
    ) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        let rx = self.supervisor.subscribe_events();
        let stream = stream::unfold(rx, |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(event) => return Some((Ok(event.into_proto()), rx)),
                    // Broadcast channels drop the oldest retained events for lagging
                    // receivers; continue from the oldest event still available.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

fn map_app_error(error: crate::error::RuntimeError) -> Status {
    Status::failed_precondition(error.to_string())
}

#[allow(clippy::result_large_err)]
fn observe_early_status<T>(method: &'static str, result: Result<T, Status>) -> Result<T, Status> {
    match result {
        Ok(value) => Ok(value),
        Err(status) => {
            observability::global().record_rpc(method, status.code());
            Err(status)
        }
    }
}

#[allow(clippy::result_large_err)]
fn validate_non_empty(field: &'static str, value: &str) -> Result<(), Status> {
    if value.trim().is_empty() {
        Err(Status::invalid_argument(format!("{field} is required")))
    } else {
        Ok(())
    }
}

#[allow(clippy::result_large_err)]
fn map_voice_context(voice: join_voice_request::VoiceContext) -> Result<VoiceContext, Status> {
    validate_non_empty("guild_id", &voice.guild_id)?;
    validate_non_empty("channel_id", &voice.channel_id)?;
    validate_non_empty("user_id", &voice.user_id)?;
    validate_non_empty("session_id", &voice.session_id)?;
    validate_non_empty("endpoint", &voice.endpoint)?;
    validate_non_empty("token", &voice.token)?;

    Ok(VoiceContext {
        guild_id: voice.guild_id,
        channel_id: voice.channel_id,
        user_id: voice.user_id,
        session_id: voice.session_id,
        endpoint: voice.endpoint,
        token: voice.token,
    })
}

fn map_session_state(state: SessionState) -> ProtoSessionState {
    match state {
        SessionState::Idle => ProtoSessionState::Idle,
        SessionState::ConnectingVoice => ProtoSessionState::ConnectingVoice,
        SessionState::VoiceReady => ProtoSessionState::VoiceReadyState,
        SessionState::ResolvingTrack => ProtoSessionState::ResolvingTrack,
        SessionState::Buffering => ProtoSessionState::BufferingState,
        SessionState::Playing => ProtoSessionState::PlayingState,
        SessionState::Paused => ProtoSessionState::PausedState,
        SessionState::Stopping => ProtoSessionState::Stopping,
        SessionState::Error => ProtoSessionState::ErrorState,
    }
}
