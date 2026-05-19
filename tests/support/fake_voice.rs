use std::sync::{Arc, Mutex as StdMutex};

use discord_voice_service::session::supervisor::VoiceContext;
use futures::StreamExt;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Instant};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;

pub struct FakeVoiceEndpoint {
    endpoint: String,
    gateway_path: Arc<StdMutex<Option<String>>>,
    discovery_count: Arc<Mutex<usize>>,
}

impl FakeVoiceEndpoint {
    pub async fn spawn() -> Self {
        Self::spawn_with_gateway_delay(Duration::ZERO).await
    }

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

                if len == 74 && &buf[..2] == &1u16.to_be_bytes() {
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

            while let Some(message) = ws.next().await {
                match message {
                    Ok(Message::Text(_)) => {}
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });

        Self {
            endpoint: format!("ws://{ws_addr}/?udp={udp_addr}&ssrc=7"),
            gateway_path,
            discovery_count,
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
