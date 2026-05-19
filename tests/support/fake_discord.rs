#![allow(dead_code)]

use std::sync::{Arc, Mutex as StdMutex};

use discord_voice_service::session::supervisor::VoiceContext;
use futures::StreamExt;
use serde_json::Value;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, Instant, sleep};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

pub struct FakeDiscordPeer {
    endpoint: String,
    gateway_path: Arc<StdMutex<Option<String>>>,
    discovery_count: Arc<Mutex<usize>>,
    speaking_observed: Arc<Notify>,
    audio_frame_count: Arc<Mutex<usize>>,
}

impl FakeDiscordPeer {
    #[allow(clippy::result_large_err)]
    pub async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let udp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = listener.local_addr().unwrap();
        let udp_addr = udp_socket.local_addr().unwrap();
        let gateway_path = Arc::new(StdMutex::new(None));
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
            gateway_path,
            discovery_count,
            speaking_observed,
            audio_frame_count,
        }
    }

    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    pub fn voice_context(
        &self,
        guild_id: &str,
        channel_id: &str,
        session_id: &str,
        token: &str,
    ) -> VoiceContext {
        VoiceContext {
            guild_id: guild_id.into(),
            channel_id: channel_id.into(),
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
