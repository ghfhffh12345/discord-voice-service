use anyhow::{Context, Result};
use serde::Serialize;
use std::fs;

use discord_voice_service_twilight::{SessionEvent, SessionEventKind};

#[derive(Debug, Clone, Default)]
pub struct LiveContractState {
    pub validated_join_voice: bool,
    pub validated_update_voice_context: bool,
    pub validated_play: bool,
    pub validated_pause: bool,
    pub validated_resume: bool,
    pub observer_proved_pause: bool,
    pub observer_proved_resume: bool,
    pub validated_stop: bool,
    pub validated_leave_voice: bool,
    pub validated_get_state: bool,
    pub validated_subscribe_events: bool,
    pub saw_voice_connecting: bool,
    pub saw_voice_ready: bool,
    pub saw_track_resolving: bool,
    pub saw_playing: bool,
    pub saw_track_ended: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LiveValidationEvidence {
    pub outcome: String,
    pub service_uri: String,
    pub ytmusic_addr: String,
    pub validated_join_voice: bool,
    pub validated_update_voice_context: bool,
    pub validated_play: bool,
    pub validated_pause: bool,
    pub validated_resume: bool,
    pub observer_proved_pause: bool,
    pub observer_proved_resume: bool,
    pub observer_pause_silence_ms: u64,
    pub observer_resume_packet_count: u64,
    pub validated_stop: bool,
    pub validated_leave_voice: bool,
    pub validated_get_state: bool,
    pub validated_subscribe_events: bool,
    pub saw_voice_connecting: bool,
    pub saw_voice_ready: bool,
    pub saw_track_resolving: bool,
    pub saw_playing: bool,
    pub saw_track_ended: bool,
    pub observed_packet_count: u64,
    pub decoded_audio_ms: u64,
    pub non_silent_audio_ms: u64,
    pub failure_reason: Option<String>,
}

pub fn emit_validation_evidence(evidence: &LiveValidationEvidence) -> Result<()> {
    let json = serde_json::to_string(evidence).context("serialize live validation evidence")?;
    if let Some(path) = std::env::var_os("LIVE_VALIDATION_EVIDENCE_PATH")
        && !path.is_empty()
    {
        fs::write(path, format!("{json}\n")).context("write live validation evidence file")?;
        return Ok(());
    }

    println!("{json}");
    Ok(())
}

pub fn finalize_success_evidence<State, BuildEvidence, EmitEvidence>(
    flow_result: Result<State>,
    cleanup: Result<()>,
    build_evidence: BuildEvidence,
    emit_evidence: EmitEvidence,
) -> Result<()>
where
    BuildEvidence: FnOnce(State) -> LiveValidationEvidence,
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
    pub fn mark_join_voice(&mut self) {
        self.validated_join_voice = true;
    }

    pub fn mark_update_voice_context(&mut self) {
        self.validated_update_voice_context = true;
    }

    pub fn mark_play(&mut self) {
        self.validated_play = true;
    }

    pub fn mark_pause(&mut self) {
        self.validated_pause = true;
        self.observer_proved_pause = true;
    }

    pub fn mark_resume(&mut self) {
        self.validated_resume = true;
        self.observer_proved_resume = true;
    }

    pub fn mark_stop(&mut self) {
        self.validated_stop = true;
    }

    pub fn mark_leave_voice(&mut self) {
        self.validated_leave_voice = true;
    }

    pub fn mark_get_state(&mut self) {
        self.validated_get_state = true;
    }

    pub fn mark_subscribe_events(&mut self) {
        self.validated_subscribe_events = true;
    }

    pub fn ensure_complete(&self) -> Result<()> {
        let missing = self.missing_required_coverage();
        if missing.is_empty() {
            return Ok(());
        }

        anyhow::bail!(
            "live validation missing required coverage: {}",
            missing.join(", ")
        );
    }

    pub fn observe_event(&mut self, event: SessionEvent, expected_video_id: &str) -> Result<bool> {
        let kind = event.kind;

        match kind {
            SessionEventKind::VoiceConnecting
            | SessionEventKind::TrackResolving
            | SessionEventKind::Buffering
            | SessionEventKind::Stopped
                if self.saw_playing && !self.saw_track_ended =>
            {
                anyhow::bail!(
                    "playback left steady Playing state after start: {}",
                    kind.as_str_name()
                );
            }
            SessionEventKind::VoiceConnecting => {
                self.saw_voice_connecting = true;
            }
            SessionEventKind::VoiceReady => {
                self.saw_voice_ready = true;
            }
            SessionEventKind::TrackResolving => {
                validate_expected_video_id(&event, expected_video_id, kind)?;
                self.saw_track_resolving = true;
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
                if !self.saw_track_resolving {
                    anyhow::bail!("TrackEnded observed before TrackResolving");
                }
                validate_expected_video_id(&event, expected_video_id, kind)?;
                self.saw_track_ended = true;

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
            SessionEventKind::VoiceReconnecting if self.saw_playing && !self.saw_track_ended => {
                anyhow::bail!(
                    "VoiceReconnecting observed: {}",
                    display_event_message(&event)
                );
            }
            _ => {}
        }

        Ok(false)
    }

    fn missing_required_coverage(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if !self.validated_join_voice {
            missing.push("JoinVoice");
        }
        if !self.validated_update_voice_context {
            missing.push("UpdateVoiceContext");
        }
        if !self.validated_play {
            missing.push("Play");
        }
        if !self.validated_pause {
            missing.push("Pause");
        }
        if !self.validated_resume {
            missing.push("Resume");
        }
        if !self.observer_proved_pause {
            missing.push("Pause observer proof");
        }
        if !self.observer_proved_resume {
            missing.push("Resume observer proof");
        }
        if !self.validated_stop {
            missing.push("Stop");
        }
        if !self.validated_leave_voice {
            missing.push("LeaveVoice");
        }
        if !self.validated_get_state {
            missing.push("GetState");
        }
        if !self.validated_subscribe_events {
            missing.push("SubscribeEvents");
        }
        if !self.saw_voice_connecting {
            missing.push("VoiceConnecting");
        }
        if !self.saw_voice_ready {
            missing.push("VoiceReady");
        }
        if !self.saw_track_resolving {
            missing.push("TrackResolving");
        }
        if !self.saw_playing {
            missing.push("Playing");
        }
        if !self.saw_track_ended {
            missing.push("TrackEnded");
        }
        missing
    }
}

fn validate_expected_video_id(
    event: &SessionEvent,
    expected_video_id: &str,
    kind: SessionEventKind,
) -> Result<()> {
    let current_video_id = event.current_video_id.as_deref().unwrap_or("").trim();
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
    if event.message.as_deref().unwrap_or("").trim().is_empty() {
        "no message".to_owned()
    } else {
        event.message.as_ref().unwrap().clone()
    }
}
