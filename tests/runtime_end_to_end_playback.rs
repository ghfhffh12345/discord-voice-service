#[path = "support/fake_ytmusic.rs"]
mod fake_ytmusic;
#[path = "support/fixtures.rs"]
mod fixtures;

use std::sync::Arc;

use discord_voice_service::session::events::SessionEventKind;
use discord_voice_service::session::state::SessionState;
use discord_voice_service::session::supervisor::{Command, Supervisor, VoiceContext};
use futures::StreamExt;
use serde_json::Value;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, Instant, sleep};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use self::fake_ytmusic::FakeYtMusic;
use self::fixtures::spawn_stream_server;

#[tokio::test]
async fn join_voice_then_play_reaches_connected_runtime_playback_path() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("tests/fixtures/audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordVoice::spawn().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut rx = supervisor.subscribe_events();

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context(),
        })
        .await
        .unwrap();

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let events = collect_events(&mut rx, 4).await;
    assert_eq!(events[0].kind, SessionEventKind::VoiceReady);
    assert_eq!(events[1].kind, SessionEventKind::TrackResolving);
    assert_eq!(events[2].kind, SessionEventKind::Playing);
    assert_eq!(events[2].current_video_id.as_deref(), Some("video-1"));
    assert_eq!(events[2].selected_itag, Some(250));
    assert_eq!(events[3].kind, SessionEventKind::TrackEnded);
    assert_eq!(events[3].current_video_id.as_deref(), Some("video-1"));
    assert_eq!(events[3].selected_itag, Some(250));

    let snapshot = supervisor.snapshot().await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert_eq!(snapshot.selected_itag, None);
    assert_eq!(snapshot.position_ms, 0);

    assert_eq!(fake_voice.discovery_count().await, 1);
    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");
    assert!(fake_voice.audio_frame_count_at_least(5).await >= 5);
}

async fn collect_events(
    rx: &mut tokio::sync::broadcast::Receiver<
        discord_voice_service::session::events::SessionEventRecord,
    >,
    count: usize,
) -> Vec<discord_voice_service::session::events::SessionEventRecord> {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut events = Vec::with_capacity(count);
    while events.len() < count && Instant::now() < deadline {
        if let Ok(event) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            events.push(event.unwrap());
        }
    }
    events
}

struct FakeDiscordVoice {
    endpoint: String,
    discovery_count: Arc<Mutex<usize>>,
    speaking_observed: Arc<Notify>,
    audio_frame_count: Arc<Mutex<usize>>,
}

impl FakeDiscordVoice {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let udp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = listener.local_addr().unwrap();
        let udp_addr = udp_socket.local_addr().unwrap();
        let discovery_count = Arc::new(Mutex::new(0usize));
        let speaking_observed = Arc::new(Notify::new());
        let audio_frame_count = Arc::new(Mutex::new(0usize));

        let discovery_count_state = Arc::clone(&discovery_count);
        let audio_frame_count_state = Arc::clone(&audio_frame_count);

        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            loop {
                let Ok((len, from)) = udp_socket.recv_from(&mut buf).await else {
                    break;
                };

                if len == 74 && &buf[..2] == &1u16.to_be_bytes() {
                    *discovery_count_state.lock().await += 1;

                    let mut response = [0u8; 74];
                    response[..2].copy_from_slice(&2u16.to_be_bytes());
                    response[2..4].copy_from_slice(&70u16.to_be_bytes());
                    response[4..8].copy_from_slice(&buf[4..8]);
                    response[8..17].copy_from_slice(b"127.0.0.1");
                    response[72..74].copy_from_slice(&from.port().to_be_bytes());
                    udp_socket.send_to(&response, from).await.unwrap();
                    continue;
                }

                if len >= 12 {
                    *audio_frame_count_state.lock().await += 1;
                }
            }
        });

        let speaking_observed_state = Arc::clone(&speaking_observed);

        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let mut ws = accept_async(stream).await.unwrap();

            while let Some(message) = ws.next().await {
                let Ok(message) = message else {
                    break;
                };
                if let Message::Text(text) = message {
                    let payload: Value = serde_json::from_str(text.as_ref()).unwrap();
                    if payload.get("op").and_then(Value::as_u64) == Some(5) {
                        speaking_observed_state.notify_one();
                    }
                }
            }
        });

        Self {
            endpoint: format!("ws://{ws_addr}/?udp={udp_addr}&ssrc=7"),
            discovery_count,
            speaking_observed,
            audio_frame_count,
        }
    }

    fn voice_context(&self) -> VoiceContext {
        VoiceContext {
            guild_id: "1".into(),
            channel_id: "2".into(),
            session_id: "session-1".into(),
            endpoint: self.endpoint.clone(),
            token: "token-1".into(),
        }
    }

    async fn discovery_count(&self) -> usize {
        wait_for_value(&self.discovery_count, |count| *count >= 1).await
    }

    fn speaking_observed(&self) -> Arc<Notify> {
        Arc::clone(&self.speaking_observed)
    }

    async fn audio_frame_count_at_least(&self, minimum: usize) -> usize {
        wait_for_value(&self.audio_frame_count, |count| *count >= minimum).await
    }
}

async fn wait_for_value<T, F>(slot: &Arc<Mutex<T>>, ready: F) -> T
where
    T: Clone,
    F: Fn(&T) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let value = slot.lock().await.clone();
        if ready(&value) || Instant::now() >= deadline {
            return value;
        }
        sleep(Duration::from_millis(10)).await;
    }
}
