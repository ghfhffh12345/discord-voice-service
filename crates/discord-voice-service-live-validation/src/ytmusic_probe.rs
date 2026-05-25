use std::time::Duration;

use anyhow::Context;
use tonic::transport::Endpoint;

pub async fn probe_ytmusic_public_grpc(endpoint: &str) -> anyhow::Result<()> {
    let endpoint = endpoint.to_owned();
    let builder = Endpoint::from_shared(endpoint.clone())
        .with_context(|| format!("failed to parse ytmusic public gRPC endpoint: {endpoint}"))?
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(3));

    builder.connect().await.with_context(|| {
        format!("failed to connect to ytmusic public gRPC endpoint: {endpoint}")
    })?;

    Ok(())
}
