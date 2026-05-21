use std::sync::{Arc, Mutex as StdMutex};

use discord_voice_service_runtime::VoiceContext;
use discord_voice_service_voice::crypto::{PREFERRED_MODE, REQUIRED_MODE};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

pub struct FakeVoiceEndpoint {
    endpoint_host: String,
    gateway_path: Arc<StdMutex<Option<String>>>,
    discovery_count: Arc<Mutex<usize>>,
}

impl FakeVoiceEndpoint {
    pub async fn spawn() -> Self {
        Self::spawn_with_gateway_delay(Duration::ZERO).await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_gateway_delay(delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let udp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = listener.local_addr().unwrap();
        let udp_addr = udp_socket.local_addr().unwrap();
        let gateway_path = Arc::new(StdMutex::new(None));
        let discovery_count = Arc::new(Mutex::new(0usize));
        let gateway_path_state = Arc::clone(&gateway_path);
        let discovery_count_state = Arc::clone(&discovery_count);

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
                }
            }
        });

        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            sleep(delay).await;
            let mut ws = accept_hdr_async(stream, move |request: &Request, response: Response| {
                *gateway_path_state.lock().unwrap() = Some(request.uri().to_string());
                Ok(response)
            })
            .await
            .unwrap();

            ws.send(Message::Text(
                json!({
                    "op": 8,
                    "d": { "heartbeat_interval": 1_000 }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();

            while let Some(message) = ws.next().await {
                match message {
                    Ok(Message::Text(text)) => {
                        let payload: Value = serde_json::from_str(text.as_ref()).unwrap();
                        match payload.get("op").and_then(Value::as_u64) {
                            Some(0) => {
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
                            Some(1) => {
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
                            _ => {}
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });

        Self {
            endpoint_host: ws_addr.to_string(),
            gateway_path,
            discovery_count,
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

    pub async fn discovery_count(&self) -> usize {
        wait_for_value(&self.discovery_count, |count| *count >= 1).await
    }

    pub async fn gateway_connected(&self) -> bool {
        wait_for_sync_value(&self.gateway_path).await.is_some()
    }

    pub async fn gateway_path(&self) -> Option<String> {
        wait_for_sync_value(&self.gateway_path).await
    }
}

async fn wait_for_value<T, F>(slot: &Arc<Mutex<T>>, ready: F) -> T
where
    T: Clone,
    F: Fn(&T) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let value = slot.lock().await.clone();
        if ready(&value) || Instant::now() >= deadline {
            return value;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_sync_value<T: Clone>(slot: &Arc<StdMutex<Option<T>>>) -> Option<T> {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let value = slot.lock().unwrap().clone();
        if value.is_some() || Instant::now() >= deadline {
            return value;
        }
        sleep(Duration::from_millis(10)).await;
    }
}
