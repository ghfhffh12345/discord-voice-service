#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaveContext {
    pub protocol_version: u32,
    pub transition_id: Option<String>,
}
