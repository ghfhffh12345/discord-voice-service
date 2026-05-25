use anyhow::{Context, Result};
use serde::Serialize;

use discord_voice_service_proto::discordvoice::v1::{SessionEvent, SessionEventKind};

#[derive(Debug, Default)]
pub struct LiveContractState {
    pub saw_voice_ready: bool,
    pub saw_playing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LiveValidationEvidence {
    pub outcome: String,
    pub service_uri: String,
    pub ytmusic_addr: String,
    pub saw_voice_ready: bool,
    pub saw_playing: bool,
    pub saw_track_ended: bool,
    pub failure_reason: Option<String>,
}

pub fn emit_validation_evidence(evidence: &LiveValidationEvidence) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string(evidence).context("serialize live validation evidence")?
    );
    Ok(())
}

pub fn finalize_success_evidence<BuildEvidence, EmitEvidence>(
    flow_result: Result<LiveContractState>,
    cleanup: Result<()>,
    build_evidence: BuildEvidence,
    emit_evidence: EmitEvidence,
) -> Result<()>
where
    BuildEvidence: FnOnce(LiveContractState) -> LiveValidationEvidence,
    EmitEvidence: FnOnce(&LiveValidationEvidence) -> Result<()>,
{
    match (flow_result, cleanup) {
        (Ok(state), Ok(())) => {
            let evidence = build_evidence(state);
            emit_evidence(&evidence)
        }
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(cleanup_error)) => Err(cleanup_error),
        (Err(primary), Err(cleanup_error)) => {
            Err(primary.context(format!("cleanup also failed: {cleanup_error}")))
        }
    }
}

impl LiveContractState {
    pub fn observe_event(&mut self, event: SessionEvent, expected_video_id: &str) -> Result<bool> {
        let kind = SessionEventKind::try_from(event.kind).unwrap_or(SessionEventKind::Unspecified);

        match kind {
            SessionEventKind::VoiceReady => {
                self.saw_voice_ready = true;
            }
            SessionEventKind::Playing if !self.saw_playing => {
                validate_expected_video_id(&event, expected_video_id, kind)?;
                self.saw_playing = true;
            }
            SessionEventKind::Playing => {
                validate_expected_video_id(&event, expected_video_id, kind)?;
            }
            SessionEventKind::TrackEnded => {
                if !self.saw_voice_ready {
                    anyhow::bail!("TrackEnded observed before VoiceReady");
                }
                if !self.saw_playing {
                    anyhow::bail!("TrackEnded observed before Playing");
                }
                validate_expected_video_id(&event, expected_video_id, kind)?;

                return Ok(true);
            }
            SessionEventKind::FatalError => {
                anyhow::bail!("FatalError observed: {}", display_event_message(&event));
            }
            SessionEventKind::PlaybackInterrupted => {
                anyhow::bail!(
                    "PlaybackInterrupted observed: {}",
                    display_event_message(&event)
                );
            }
            SessionEventKind::VoiceReconnecting => {
                anyhow::bail!(
                    "VoiceReconnecting observed: {}",
                    display_event_message(&event)
                );
            }
            SessionEventKind::VoiceConnecting
            | SessionEventKind::TrackResolving
            | SessionEventKind::Buffering
            | SessionEventKind::Paused
            | SessionEventKind::Stopped
                if self.saw_playing =>
            {
                anyhow::bail!(
                    "playback left steady Playing state after start: {}",
                    kind.as_str_name()
                );
            }
            _ => {}
        }

        Ok(false)
    }
}

fn validate_expected_video_id(
    event: &SessionEvent,
    expected_video_id: &str,
    kind: SessionEventKind,
) -> Result<()> {
    let current_video_id = event.current_video_id.trim();
    if current_video_id == expected_video_id {
        return Ok(());
    }

    let observed = if current_video_id.is_empty() {
        "none".to_owned()
    } else {
        current_video_id.to_owned()
    };
    anyhow::bail!(
        "{} observed wrong current_video_id: expected video `{expected_video_id}`, got `{observed}`",
        kind.as_str_name()
    );
}

fn display_event_message(event: &SessionEvent) -> String {
    if event.message.trim().is_empty() {
        "no message".to_owned()
    } else {
        event.message.clone()
    }
}
