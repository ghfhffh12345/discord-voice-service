use anyhow::{Context, Result};
use discord_voice_service_live_validation::probe_ytmusic_public_grpc;

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint = std::env::args()
        .nth(1)
        .context("usage: ytmusic_ready_check <http://host:port>")?;
    probe_ytmusic_public_grpc(&endpoint).await
}
