use futures::SinkExt;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::error::AppError;

pub type VoiceWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect(url: &str) -> Result<VoiceWebSocket, AppError> {
    let (ws, _) = connect_async(url).await?;
    Ok(ws)
}

pub async fn send_json(ws: &mut VoiceWebSocket, payload: Value) -> Result<(), AppError> {
    ws.send(Message::Text(payload.to_string().into())).await?;
    Ok(())
}
