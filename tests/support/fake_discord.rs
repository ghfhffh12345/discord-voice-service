#![allow(dead_code)]

use std::sync::{Arc, Mutex as StdMutex};

use discord_voice_service::discord_voice::crypto::{PREFERRED_MODE, REQUIRED_MODE};
use discord_voice_service::session::supervisor::VoiceContext;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, Instant, sleep};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

pub struct FakeDiscordPeer {
    endpoint_host: String,
    gateway_path: Arc<StdMutex<Option<String>>>,
    discovery_count: Arc<Mutex<usize>>,
    speaking_observed: Arc<Notify>,
    audio_frame_count: Arc<Mutex<usize>>,
    heartbeat_count: Arc<Mutex<usize>>,
    saw_identify: Arc<Mutex<bool>>,
    saw_resume: Arc<Mutex<bool>>,
    saw_select_protocol: Arc<Mutex<bool>>,
    session_description_sent: Arc<Mutex<bool>>,
}

impl FakeDiscordPeer {
    #[allow(clippy::result_large_err)]
    pub async fn spawn() -> Self {
        Self::spawn_real_shape().await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_real_shape() -> Self {
        Self::spawn_real_shape_with_heartbeat_interval(1_000).await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_real_shape_with_heartbeat_interval(heartbeat_interval_ms: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let udp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = listener.local_addr().unwrap();
        let udp_addr = udp_socket.local_addr().unwrap();
        let gateway_path = Arc::new(StdMutex::new(None));
        let discovery_count = Arc::new(Mutex::new(0usize));
        let speaking_observed = Arc::new(Notify::new());
        let audio_frame_count = Arc::new(Mutex::new(0usize));
        let heartbeat_count = Arc::new(Mutex::new(0usize));
        let saw_identify = Arc::new(Mutex::new(false));
        let saw_resume = Arc::new(Mutex::new(false));
        let saw_select_protocol = Arc::new(Mutex::new(false));
        let session_description_sent = Arc::new(Mutex::new(false));

        let discovery_count_state = Arc::clone(&discovery_count);
        let audio_frame_count_state = Arc::clone(&audio_frame_count);
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            loop {
                let Ok((len, from)) = udp_socket.recv_from(&mut buf).await else {
                    break;
                };

                if len == 74 && buf[..2] == 1u16.to_be_bytes() {
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

        let gateway_path_state = Arc::clone(&gateway_path);
        let speaking_observed_state = Arc::clone(&speaking_observed);
        let heartbeat_count_state = Arc::clone(&heartbeat_count);
        let saw_identify_state = Arc::clone(&saw_identify);
        let saw_resume_state = Arc::clone(&saw_resume);
        let saw_select_protocol_state = Arc::clone(&saw_select_protocol);
        let session_description_state = Arc::clone(&session_description_sent);
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let mut ws = accept_hdr_async(stream, move |request: &Request, response: Response| {
                *gateway_path_state.lock().unwrap() = Some(request.uri().to_string());
                Ok(response)
            })
            .await
            .unwrap();

            ws.send(Message::Text(
                json!({
                    "op": 8,
                    "d": { "heartbeat_interval": heartbeat_interval_ms }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();

            while let Some(message) = ws.next().await {
                let Ok(message) = message else {
                    break;
                };
                if let Message::Text(text) = message {
                    let payload: Value = serde_json::from_str(text.as_ref()).unwrap();
                    match payload.get("op").and_then(Value::as_u64) {
                        Some(0) => {
                            let identify = payload.get("d").cloned().unwrap_or(Value::Null);
                            let required_fields_present = identify
                                .get("server_id")
                                .and_then(Value::as_str)
                                .is_some_and(|value| !value.is_empty())
                                && identify
                                    .get("user_id")
                                    .and_then(Value::as_str)
                                    .is_some_and(|value| !value.is_empty())
                                && identify
                                    .get("session_id")
                                    .and_then(Value::as_str)
                                    .is_some_and(|value| !value.is_empty())
                                && identify
                                    .get("token")
                                    .and_then(Value::as_str)
                                    .is_some_and(|value| !value.is_empty());
                            *saw_identify_state.lock().await = required_fields_present;
                            if !required_fields_present {
                                continue;
                            }
                            ws.send(Message::Text(
                                json!({
                                    "op": 2,
                                    "d": {
                                        "ssrc": 7,
                                        "ip": udp_addr.ip().to_string(),
                                        "port": udp_addr.port(),
                                        "modes": [PREFERRED_MODE, REQUIRED_MODE],
                                    }
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                        }
                        Some(7) => {
                            *saw_resume_state.lock().await = true;
                            ws.send(Message::Text(
                                json!({
                                    "op": 9,
                                    "d": {}
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                        }
                        Some(1) => {
                            *saw_select_protocol_state.lock().await = true;
                            *session_description_state.lock().await = true;
                            let mode = payload
                                .pointer("/d/data/mode")
                                .and_then(Value::as_str)
                                .unwrap_or(PREFERRED_MODE);
                            ws.send(Message::Text(
                                json!({
                                    "op": 4,
                                    "d": {
                                        "mode": mode,
                                        "secret_key": vec![0u8; 32],
                                    }
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                        }
                        Some(5) => speaking_observed_state.notify_one(),
                        Some(3) => *heartbeat_count_state.lock().await += 1,
                        _ => {}
                    }
                }
            }
        });

        Self {
            endpoint_host: ws_addr.to_string(),
            gateway_path,
            discovery_count,
            speaking_observed,
            audio_frame_count,
            heartbeat_count,
            saw_identify,
            saw_resume,
            saw_select_protocol,
            session_description_sent,
        }
    }

    pub fn endpoint(&self) -> String {
        self.endpoint_host.clone()
    }

    pub fn voice_context(
        &self,
        guild_id: &str,
        channel_id: &str,
        user_id: &str,
        session_id: &str,
        token: &str,
    ) -> VoiceContext {
        VoiceContext {
            guild_id: guild_id.into(),
            channel_id: channel_id.into(),
            user_id: user_id.into(),
            session_id: session_id.into(),
            endpoint: self.endpoint(),
            token: token.into(),
        }
    }

    pub async fn gateway_path(&self) -> Option<String> {
        wait_for_sync_value(&self.gateway_path).await
    }

    pub async fn discovery_count(&self) -> usize {
        wait_for_value(&self.discovery_count, |count| *count >= 1).await
    }

    pub fn speaking_observed(&self) -> Arc<Notify> {
        Arc::clone(&self.speaking_observed)
    }

    pub async fn audio_frame_count_at_least(&self, minimum: usize) -> usize {
        wait_for_value(&self.audio_frame_count, |count| *count >= minimum).await
    }

    pub async fn heartbeat_count_at_least(&self, minimum: usize) -> usize {
        wait_for_value(&self.heartbeat_count, |count| *count >= minimum).await
    }

    pub async fn saw_identify(&self) -> bool {
        wait_for_value(&self.saw_identify, |ready| *ready).await
    }

    pub async fn saw_resume(&self) -> bool {
        wait_for_value(&self.saw_resume, |ready| *ready).await
    }

    pub async fn saw_select_protocol(&self) -> bool {
        wait_for_value(&self.saw_select_protocol, |ready| *ready).await
    }

    pub async fn session_description_sent(&self) -> bool {
        wait_for_value(&self.session_description_sent, |ready| *ready).await
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

async fn wait_for_sync_value<T: Clone>(slot: &Arc<StdMutex<Option<T>>>) -> Option<T> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let value = slot.lock().unwrap().clone();
        if value.is_some() || Instant::now() >= deadline {
            return value;
        }
        sleep(Duration::from_millis(10)).await;
    }
}
