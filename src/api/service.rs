use tokio_stream::{Empty, empty};
use tonic::{Request, Response, Status};

use crate::proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControl;
use crate::proto::discordvoice::v1::{
    GetStateRequest, JoinVoiceRequest, JoinVoiceResponse, LeaveVoiceRequest, LeaveVoiceResponse,
    PauseRequest, PauseResponse, PlayRequest, PlayResponse, ResumeRequest, ResumeResponse,
    SessionEvent, SessionState as ProtoSessionState, SessionStateSnapshot, StopRequest,
    StopResponse, SubscribeEventsRequest,
};
use crate::session::state::SessionState;
use crate::session::supervisor::{Command, Supervisor};

pub fn map_play_request(request: PlayRequest) -> String {
    request.video_id
}

pub struct ControlService {
    pub supervisor: Supervisor,
}

#[tonic::async_trait]
impl DiscordVoiceControl for ControlService {
    type SubscribeEventsStream = Empty<Result<SessionEvent, Status>>;

    async fn join_voice(
        &self,
        request: Request<JoinVoiceRequest>,
    ) -> Result<Response<JoinVoiceResponse>, Status> {
        let voice = request
            .into_inner()
            .voice
            .ok_or_else(|| Status::invalid_argument("missing voice context"))?;

        self.supervisor
            .send(Command::JoinVoice {
                guild_id: voice.guild_id,
                channel_id: voice.channel_id,
                session_id: voice.session_id,
                endpoint: voice.endpoint,
                token: voice.token,
            })
            .await
            .map_err(map_app_error)?;

        Ok(Response::new(JoinVoiceResponse {}))
    }

    async fn play(&self, request: Request<PlayRequest>) -> Result<Response<PlayResponse>, Status> {
        self.supervisor
            .send(Command::Play {
                video_id: request.into_inner().video_id,
            })
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;

        Ok(Response::new(PlayResponse {}))
    }

    async fn pause(
        &self,
        _request: Request<PauseRequest>,
    ) -> Result<Response<PauseResponse>, Status> {
        self.supervisor
            .send(Command::Pause)
            .await
            .map_err(map_app_error)?;
        Ok(Response::new(PauseResponse {}))
    }

    async fn resume(
        &self,
        _request: Request<ResumeRequest>,
    ) -> Result<Response<ResumeResponse>, Status> {
        self.supervisor
            .send(Command::Resume)
            .await
            .map_err(map_app_error)?;
        Ok(Response::new(ResumeResponse {}))
    }

    async fn stop(&self, _request: Request<StopRequest>) -> Result<Response<StopResponse>, Status> {
        self.supervisor
            .send(Command::Stop)
            .await
            .map_err(map_app_error)?;
        Ok(Response::new(StopResponse {}))
    }

    async fn leave_voice(
        &self,
        _request: Request<LeaveVoiceRequest>,
    ) -> Result<Response<LeaveVoiceResponse>, Status> {
        self.supervisor
            .send(Command::LeaveVoice)
            .await
            .map_err(map_app_error)?;
        Ok(Response::new(LeaveVoiceResponse {}))
    }

    async fn get_state(
        &self,
        _request: Request<GetStateRequest>,
    ) -> Result<Response<SessionStateSnapshot>, Status> {
        let snapshot = self.supervisor.snapshot().await;
        Ok(Response::new(SessionStateSnapshot {
            state: map_session_state(snapshot.state) as i32,
            guild_id: snapshot.guild_id.unwrap_or_default(),
            channel_id: snapshot.channel_id.unwrap_or_default(),
            current_video_id: snapshot.current_video_id.unwrap_or_default(),
            queue_depth: 0,
            selected_itag: 0,
            message: String::new(),
        }))
    }

    async fn subscribe_events(
        &self,
        _request: Request<SubscribeEventsRequest>,
    ) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        Ok(Response::new(empty()))
    }
}

fn map_app_error(error: crate::error::AppError) -> Status {
    Status::failed_precondition(error.to_string())
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
