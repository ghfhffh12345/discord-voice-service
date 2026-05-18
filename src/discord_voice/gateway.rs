#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceContext {
    pub guild_id: String,
    pub channel_id: String,
    pub session_id: String,
    pub endpoint: String,
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDescription {
    pub mode: String,
    pub secret_key: Vec<u8>,
}
