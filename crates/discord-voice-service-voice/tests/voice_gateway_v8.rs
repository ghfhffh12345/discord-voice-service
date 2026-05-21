use std::sync::{Arc, Mutex as StdMutex};

use discord_voice_service_voice::test_support::{GatewayEvent, VoiceGatewayClient};
use futures::StreamExt;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

struct FakeVoiceGateway {
    url: String,
    request_path: Arc<StdMutex<Option<String>>>,
    heartbeat_seq_ack: Arc<Mutex<Option<i64>>>,
    resume_seq_ack: Arc<Mutex<Option<i64>>>,
    heartbeat_count: Arc<Mutex<usize>>,
}

impl FakeVoiceGateway {
    #[allow(clippy::result_large_err)]
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let request_path = Arc::new(StdMutex::new(None));
        let heartbeat_seq_ack = Arc::new(Mutex::new(None));
        let resume_seq_ack = Arc::new(Mutex::new(None));
        let heartbeat_count = Arc::new(Mutex::new(0usize));
        let request_path_state = Arc::clone(&request_path);
        let heartbeat_state = Arc::clone(&heartbeat_seq_ack);
        let resume_state = Arc::clone(&resume_seq_ack);
        let heartbeat_count_state = Arc::clone(&heartbeat_count);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_hdr_async(stream, move |request: &Request, response: Response| {
                *request_path_state.lock().unwrap() = Some(request.uri().to_string());
                Ok(response)
            })
            .await
            .unwrap();

            while let Some(message) = ws.next().await {
                let message = message.unwrap();
                if let Message::Text(text) = message {
                    let payload: Value = serde_json::from_str(text.as_ref()).unwrap();
                    let seq_ack = payload
                        .get("d")
                        .and_then(|data| data.get("seq_ack"))
                        .and_then(Value::as_i64);

                    match payload.get("op").and_then(Value::as_u64) {
                        Some(3) => {
                            *heartbeat_state.lock().await = seq_ack;
                            *heartbeat_count_state.lock().await += 1;
                        }
                        Some(7) => *resume_state.lock().await = seq_ack,
                        _ => {}
                    }
                }
            }
        });

        Self {
            url: format!("ws://{addr}"),
            request_path,
            heartbeat_seq_ack,
            resume_seq_ack,
            heartbeat_count,
        }
    }

    fn url(&self) -> &str {
        &self.url
    }

    async fn request_path(&self) -> Option<String> {
        wait_for_sync_value(&self.request_path).await
    }

    async fn last_heartbeat_seq_ack(&self) -> Option<i64> {
        wait_for_value(&self.heartbeat_seq_ack).await
    }

    async fn last_resume_seq_ack(&self) -> Option<i64> {
        wait_for_value(&self.resume_seq_ack).await
    }

    async fn heartbeat_count_at_least(&self, minimum: usize) -> usize {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let count = *self.heartbeat_count.lock().await;
            if count >= minimum || Instant::now() >= deadline {
                return count;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn wait_for_value<T: Clone>(slot: &Arc<Mutex<Option<T>>>) -> Option<T> {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let value = slot.lock().await.clone();
        if value.is_some() || Instant::now() >= deadline {
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

#[tokio::test]
async fn voice_gateway_v8_heartbeat_and_resume_include_seq_ack() {
    let fake = FakeVoiceGateway::spawn().await;
    let client = VoiceGatewayClient::connect(fake.url()).await.unwrap();

    client
        .apply_gateway_event(&GatewayEvent::new(Some(42)))
        .await;
    client.send_heartbeat().await.unwrap();
    client
        .send_resume("server-id", "session-id", "token")
        .await
        .unwrap();

    assert_eq!(
        fake.request_path().await.as_deref(),
        Some("/?v=8&encoding=json")
    );
    assert_eq!(fake.last_heartbeat_seq_ack().await, Some(42));
    assert_eq!(fake.last_resume_seq_ack().await, Some(42));
}

#[tokio::test]
async fn voice_gateway_v8_encodes_missing_seq_ack_as_minus_one() {
    let fake = FakeVoiceGateway::spawn().await;
    let client = VoiceGatewayClient::connect(fake.url()).await.unwrap();

    client.send_heartbeat().await.unwrap();
    client
        .send_resume("server-id", "session-id", "token")
        .await
        .unwrap();

    assert_eq!(fake.last_heartbeat_seq_ack().await, Some(-1));
    assert_eq!(fake.last_resume_seq_ack().await, Some(-1));
}

#[tokio::test]
async fn voice_gateway_can_send_heartbeat_while_receive_is_pending() {
    let fake = FakeVoiceGateway::spawn().await;
    let client = VoiceGatewayClient::connect(fake.url()).await.unwrap();
    let reader = client.clone();

    tokio::spawn(async move {
        let _ = reader.receive_event().await;
    });

    client.send_heartbeat().await.unwrap();

    assert_eq!(fake.heartbeat_count_at_least(1).await, 1);
}
