#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayEvent {
    seq: Option<u64>,
}

impl GatewayEvent {
    pub fn new(seq: Option<u64>) -> Self {
        Self { seq }
    }

    pub fn seq(&self) -> Option<u64> {
        self.seq
    }
}
