use std::sync::Arc;

use discord_voice_service::discord_voice::gateway::VoiceGatewayClient;
use futures::StreamExt;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

struct FakeVoiceGateway {
    url: String,
    heartbeat_seq_ack: Arc<Mutex<Option<u64>>>,
    resume_seq_ack: Arc<Mutex<Option<u64>>>,
}

impl FakeVoiceGateway {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let heartbeat_seq_ack = Arc::new(Mutex::new(None));
        let resume_seq_ack = Arc::new(Mutex::new(None));
        let heartbeat_state = Arc::clone(&heartbeat_seq_ack);
        let resume_state = Arc::clone(&resume_seq_ack);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();

            while let Some(message) = ws.next().await {
                let message = message.unwrap();
                if let Message::Text(text) = message {
                    let payload: Value = serde_json::from_str(text.as_ref()).unwrap();
                    let seq_ack = payload
                        .get("d")
                        .and_then(|data| data.get("seq_ack"))
                        .and_then(Value::as_u64);

                    match payload.get("op").and_then(Value::as_u64) {
                        Some(3) => *heartbeat_state.lock().await = seq_ack,
                        Some(7) => *resume_state.lock().await = seq_ack,
                        _ => {}
                    }
                }
            }
        });

        Self {
            url: format!("ws://{addr}"),
            heartbeat_seq_ack,
            resume_seq_ack,
        }
    }

    fn url(&self) -> &str {
        &self.url
    }

    async fn last_heartbeat_seq_ack(&self) -> Option<u64> {
        wait_for_seq_ack(&self.heartbeat_seq_ack).await
    }

    async fn last_resume_seq_ack(&self) -> Option<u64> {
        wait_for_seq_ack(&self.resume_seq_ack).await
    }
}

async fn wait_for_seq_ack(slot: &Arc<Mutex<Option<u64>>>) -> Option<u64> {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let seq_ack = *slot.lock().await;
        if seq_ack.is_some() || Instant::now() >= deadline {
            return seq_ack;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn voice_gateway_v8_heartbeat_and_resume_include_seq_ack() {
    let fake = FakeVoiceGateway::spawn().await;
    let mut client = VoiceGatewayClient::connect(fake.url()).await.unwrap();

    client.record_seq_ack(42);
    client.send_heartbeat().await.unwrap();
    client
        .send_resume("server-id", "session-id", "token")
        .await
        .unwrap();

    assert_eq!(fake.last_heartbeat_seq_ack().await, Some(42));
    assert_eq!(fake.last_resume_seq_ack().await, Some(42));
}
