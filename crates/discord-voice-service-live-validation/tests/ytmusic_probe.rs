use std::net::TcpListener;

use anyhow::anyhow;
use discord_voice_service_live_validation::probe_ytmusic_public_grpc;
use tokio::time::{Duration, sleep, timeout};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[tokio::test]
async fn ytmusic_probe_rejects_closed_port() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind should succeed");
    let addr = listener
        .local_addr()
        .expect("local addr should be available");
    drop(listener);

    let error = probe_ytmusic_public_grpc(&format!("http://{addr}"))
        .await
        .expect_err("closed port should fail");

    assert!(
        error
            .to_string()
            .contains("connect to ytmusic public gRPC endpoint"),
        "unexpected error: {error}",
    );
}

#[tokio::test]
async fn ytmusic_probe_accepts_health_server() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind should succeed");
    let addr = listener
        .local_addr()
        .expect("local addr should be available");
    let incoming = TcpListenerStream::new(listener);
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

    health_reporter
        .set_serving::<tonic_health::pb::health_server::HealthServer<
            tonic_health::server::HealthService,
        >>()
        .await;

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(health_service)
            .serve_with_incoming(incoming)
            .await
            .expect("health server should run");
    });

    wait_for_ready(&format!("http://{addr}"))
        .await
        .expect("health server should satisfy readiness");

    server.abort();
    let _ = server.await;
}

async fn wait_for_ready(endpoint: &str) -> anyhow::Result<()> {
    timeout(Duration::from_secs(2), async {
        loop {
            match probe_ytmusic_public_grpc(endpoint).await {
                Ok(()) => return Ok(()),
                Err(_) => sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow!("ytmusic public gRPC endpoint never became ready"))?
}
