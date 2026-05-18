#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredUdpAddress {
    pub ip: String,
    pub port: u16,
}
