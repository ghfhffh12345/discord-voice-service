use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tonic::{Code, Status};
use tracing::{Level, event};

use crate::session::{ReadinessSnapshot, Snapshot};

static OBSERVABILITY: OnceLock<Observability> = OnceLock::new();

pub fn global() -> &'static Observability {
    OBSERVABILITY.get_or_init(Observability::default)
}

#[derive(Debug)]
pub struct Observability {
    rpc_requests_total: AtomicU64,
    state_queries_total: AtomicU64,
    readiness_transitions_total: AtomicU64,
    ytmusic_probe_total: AtomicU64,
    last_ready: AtomicBool,
}

impl Default for Observability {
    fn default() -> Self {
        Self {
            rpc_requests_total: AtomicU64::new(0),
            state_queries_total: AtomicU64::new(0),
            readiness_transitions_total: AtomicU64::new(0),
            ytmusic_probe_total: AtomicU64::new(0),
            last_ready: AtomicBool::new(false),
        }
    }
}

impl Observability {
    pub fn record_rpc(&self, method: &'static str, code: Code) {
        let total = self.rpc_requests_total.fetch_add(1, Ordering::Relaxed) + 1;
        event!(
            target: "discord_voice_service.rpc",
            Level::INFO,
            method,
            code = %code,
            rpc_requests_total = total,
            "control rpc handled"
        );
    }

    pub fn record_rpc_result<T>(&self, method: &'static str, result: &Result<T, Status>) {
        match result {
            Ok(_) => self.record_rpc(method, Code::Ok),
            Err(status) => self.record_rpc(method, status.code()),
        }
    }

    pub fn record_state_query(&self, snapshot: &Snapshot, readiness: ReadinessSnapshot) {
        let total = self.state_queries_total.fetch_add(1, Ordering::Relaxed) + 1;
        event!(
            target: "discord_voice_service.state",
            Level::DEBUG,
            state = ?snapshot.state,
            ready = readiness.is_ready(),
            runtime_booted = readiness.runtime_booted,
            ytmusic_healthy = readiness.ytmusic_healthy,
            recovering = snapshot.recovering,
            voice_reconnecting = snapshot.voice_reconnecting,
            position_ms = snapshot.position_ms,
            state_queries_total = total,
            "session state queried"
        );
    }

    pub fn record_readiness(&self, readiness: ReadinessSnapshot) {
        let ready = readiness.is_ready();
        let previous = self.last_ready.swap(ready, Ordering::Relaxed);
        if previous != ready {
            let total = self
                .readiness_transitions_total
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            event!(
                target: "discord_voice_service.readiness",
                Level::INFO,
                ready,
                runtime_booted = readiness.runtime_booted,
                ytmusic_healthy = readiness.ytmusic_healthy,
                readiness_transitions_total = total,
                "readiness changed"
            );
        }
    }

    pub fn record_ytmusic_probe(&self, healthy: bool) {
        let total = self.ytmusic_probe_total.fetch_add(1, Ordering::Relaxed) + 1;
        event!(
            target: "discord_voice_service.ytmusic",
            Level::DEBUG,
            healthy,
            ytmusic_probe_total = total,
            "ytmusic reachability probe completed"
        );
    }
}

pub fn render_snapshot_message(snapshot: &Snapshot, readiness: ReadinessSnapshot) -> String {
    let mut parts = Vec::with_capacity(2);

    if let Some(reason) = snapshot.last_reason.as_deref()
        && !reason.is_empty()
    {
        parts.push(reason.to_owned());
    }

    parts.push(format!(
        "ready={} runtime_booted={} ytmusic_healthy={} recovering={} voice_reconnecting={} position_ms={}",
        readiness.is_ready(),
        readiness.runtime_booted,
        readiness.ytmusic_healthy,
        snapshot.recovering,
        snapshot.voice_reconnecting,
        snapshot.position_ms
    ));

    parts.join(" | ")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    #[test]
    fn public_probe_hook_records_metric() {
        let before = super::global().ytmusic_probe_total.load(Ordering::Relaxed);

        crate::record_ytmusic_probe(true);

        let after = super::global().ytmusic_probe_total.load(Ordering::Relaxed);
        assert_eq!(after, before + 1);
    }
}
