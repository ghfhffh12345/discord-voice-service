use futures::SinkExt;
use http::Uri;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::error::AppError;

pub type VoiceWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect(url: &str) -> Result<VoiceWebSocket, AppError> {
    let normalized_url = normalize_gateway_url(url)?;
    let (ws, _) = connect_async(normalized_url).await?;
    Ok(ws)
}

pub async fn send_json(ws: &mut VoiceWebSocket, payload: Value) -> Result<(), AppError> {
    ws.send(Message::Text(payload.to_string().into())).await?;
    Ok(())
}

fn normalize_gateway_url(url: &str) -> Result<String, AppError> {
    let uri: Uri = url.parse()?;
    let mut parts = uri.into_parts();
    if parts.scheme.is_none() || parts.authority.is_none() {
        return Err(AppError::InvalidState("voice gateway url must be absolute"));
    }

    let path = parts
        .path_and_query
        .as_ref()
        .map(|path_and_query| path_and_query.path())
        .unwrap_or("/");
    let query = normalize_query(
        parts
            .path_and_query
            .as_ref()
            .and_then(|path_and_query| path_and_query.query()),
    );
    let path_and_query = format!("{path}?{query}");
    parts.path_and_query = Some(path_and_query.parse()?);

    Ok(Uri::from_parts(parts)
        .map_err(|_| AppError::InvalidState("voice gateway url could not be normalized"))?
        .to_string())
}

fn normalize_query(query: Option<&str>) -> String {
    let mut params = Vec::new();

    if let Some(query) = query {
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
            if key != "v" && key != "encoding" {
                params.push(pair.to_owned());
            }
        }
    }

    params.push("v=8".to_owned());
    params.push("encoding=json".to_owned());
    params.join("&")
}
