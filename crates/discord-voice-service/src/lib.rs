pub mod config;

use std::sync::Arc;

use anyhow::Result;
use config::Settings;
pub use discord_voice_service_runtime::RuntimeError;
use discord_voice_service_runtime::{ControlService, Readiness, Supervisor, record_ytmusic_probe};
use tonic::transport::{Endpoint, Server};
use tonic_health::ServingStatus;
use tracing::{info, warn};

pub async fn run(settings: Settings) -> Result<()> {
    let readiness = Readiness::global();
    readiness.mark_runtime_booted().await;

    let supervisor = Supervisor::with_ytmusic_endpoint(settings.ytmusic_addr.clone()).await?;
    let control_service = ControlService {
        supervisor,
        readiness: readiness.clone(),
    };
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    set_serving_status(&mut health_reporter, false).await;

    tokio::spawn(run_ytmusic_probe_loop(
        settings.clone(),
        readiness,
        health_reporter.clone(),
    ));

    Server::builder()
        .add_service(health_service)
        .add_service(
            discord_voice_service_proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControlServer::new(control_service),
        )
        .serve(settings.bind_addr)
        .await?;

    Ok(())
}

async fn run_ytmusic_probe_loop(
    settings: Settings,
    readiness: Arc<Readiness>,
    mut health_reporter: tonic_health::server::HealthReporter,
) {
    let mut interval = tokio::time::interval(settings.ytmusic_probe_interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let healthy = probe_ytmusic(&settings).await;
        record_ytmusic_probe(healthy);

        if healthy {
            readiness.mark_ytmusic_healthy().await;
            set_serving_status(&mut health_reporter, true).await;
        } else {
            readiness.mark_ytmusic_unhealthy().await;
            set_serving_status(&mut health_reporter, false).await;
        }

        interval.tick().await;
    }
}

async fn set_serving_status(
    health_reporter: &mut tonic_health::server::HealthReporter,
    serving: bool,
) {
    let status = if serving {
        ServingStatus::Serving
    } else {
        ServingStatus::NotServing
    };
    health_reporter.set_service_status("", status).await;
    health_reporter
        .set_service_status(
            <discord_voice_service_proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControlServer<ControlService> as tonic::server::NamedService>::NAME,
            status,
        )
        .await;
}

async fn probe_ytmusic(settings: &Settings) -> bool {
    let endpoint = match Endpoint::from_shared(settings.ytmusic_addr.clone()) {
        Ok(endpoint) => endpoint
            .connect_timeout(settings.ytmusic_probe_timeout())
            .timeout(settings.ytmusic_probe_timeout()),
        Err(error) => {
            warn!(error = %error, endpoint = %settings.ytmusic_addr, "invalid ytmusic endpoint");
            return false;
        }
    };

    match endpoint.connect().await {
        Ok(_channel) => {
            info!(endpoint = %settings.ytmusic_addr, "ytmusic reachability confirmed");
            true
        }
        Err(error) => {
            warn!(error = %error, endpoint = %settings.ytmusic_addr, "ytmusic reachability probe failed");
            false
        }
    }
}
