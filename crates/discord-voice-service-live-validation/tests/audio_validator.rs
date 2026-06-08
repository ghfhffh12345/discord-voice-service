use std::time::{Duration, Instant};

use discord_voice_service_live_validation::{
    AudioValidationAccumulator, ObservedOpusPacket, analyze_opus_packets,
};
use discord_voice_service_playback::media::webm_demux::WebmOpusDemux;
use discord_voice_service_test_support::fixtures::load_fixture_bytes;
use opus_rs::{Application, OpusEncoder};

#[test]
fn audio_validator_rejects_empty_payload() {
    let error = analyze_opus_packets([ObservedOpusPacket {
        sequence: 7,
        timestamp: 0,
        payload: &[],
    }])
    .expect_err("empty payload should fail");

    assert!(error.to_string().contains("must not be empty"));
}

#[test]
fn audio_validator_rejects_malformed_payload() {
    let error = analyze_opus_packets([ObservedOpusPacket {
        sequence: 7,
        timestamp: 0,
        payload: &[0x03],
    }])
    .expect_err("malformed payload should fail");

    assert!(error.to_string().contains("unsupported opus packet header"));
}

#[test]
fn audio_validator_rejects_random_payload() {
    let error = analyze_opus_packets([ObservedOpusPacket {
        sequence: 7,
        timestamp: 0,
        payload: &[0xff, 0x00, 0xaa, 0x55],
    }])
    .expect_err("random payload should fail");

    assert!(
        error.to_string().contains("unsupported opus packet header")
            || error.to_string().contains("decode opus packet"),
        "unexpected random-payload error: {error}",
    );
}

#[test]
fn audio_validator_decodes_real_fixture_packets() {
    let mut demux = WebmOpusDemux::default();
    demux.push_bytes(load_fixture_bytes("audio-itag250.webm"));
    let packets = demux.drain_packets().expect("fixture should demux");
    assert!(!packets.is_empty(), "fixture should yield opus packets");

    let stats = analyze_opus_packets(packets.iter().enumerate().map(|(index, packet)| {
        ObservedOpusPacket {
            sequence: index as u16,
            timestamp: packet.timestamp_ms.saturating_mul(48) as u32,
            payload: packet.data.as_ref(),
        }
    }))
    .expect("fixture packets should decode");

    assert_eq!(stats.observed_packet_count as usize, packets.len());
    assert!(stats.decoded_audio_ms > 0);
    assert_eq!(stats.first_sequence, Some(0));
    assert_eq!(stats.last_sequence, Some((packets.len() - 1) as u16));
}

#[test]
fn audio_validator_rejects_rtp_timestamp_delta_that_disagrees_with_decoded_samples() {
    let mut demux = WebmOpusDemux::default();
    demux.push_bytes(load_fixture_bytes("audio-itag250.webm"));
    let packets = demux.drain_packets().expect("fixture should demux");
    assert!(
        packets.len() >= 2,
        "fixture should yield at least two packets"
    );

    let mut accumulator = AudioValidationAccumulator::new();
    accumulator
        .observe_packet(ObservedOpusPacket {
            sequence: 0,
            timestamp: 0,
            payload: packets[0].data.as_ref(),
        })
        .expect("first fixture packet should decode");

    let wrong_timestamp = packets[0].duration_samples.saturating_sub(1);
    let error = accumulator
        .observe_packet(ObservedOpusPacket {
            sequence: 1,
            timestamp: wrong_timestamp,
            payload: packets[1].data.as_ref(),
        })
        .expect_err("wrong RTP timestamp delta should fail validation");

    assert!(
        error.to_string().contains("rtp timestamp delta"),
        "unexpected error: {error}"
    );
}

#[test]
fn audio_validator_rejects_rtp_sequence_gap() {
    let mut demux = WebmOpusDemux::default();
    demux.push_bytes(load_fixture_bytes("audio-itag250.webm"));
    let packets = demux.drain_packets().expect("fixture should demux");
    assert!(
        packets.len() >= 2,
        "fixture should yield at least two packets"
    );

    let mut accumulator = AudioValidationAccumulator::new();
    accumulator
        .observe_packet(ObservedOpusPacket {
            sequence: 41,
            timestamp: 0,
            payload: packets[0].data.as_ref(),
        })
        .expect("first fixture packet should decode");

    let error = accumulator
        .observe_packet(ObservedOpusPacket {
            sequence: 43,
            timestamp: packets[0].duration_samples,
            payload: packets[1].data.as_ref(),
        })
        .expect_err("RTP sequence gap should fail validation");

    assert!(
        error.to_string().contains("rtp sequence jumped"),
        "unexpected error: {error}"
    );
}

#[test]
fn audio_validator_reports_non_silence_for_generated_opus_packet() {
    let mut encoder =
        OpusEncoder::new(48_000, 1, Application::RestrictedLowDelay).expect("create opus encoder");
    let pcm = (0..960)
        .map(|sample| ((sample as f32) * 0.1).sin() * 0.2)
        .collect::<Vec<_>>();
    let mut encoded = vec![0u8; 1500];
    let encoded_len = encoder
        .encode(&pcm, 960, &mut encoded)
        .expect("encode test opus packet");
    encoded.truncate(encoded_len);

    let stats = analyze_opus_packets([ObservedOpusPacket {
        sequence: 42,
        timestamp: 0,
        payload: encoded.as_ref(),
    }])
    .expect("encoded packet should decode");

    assert_eq!(stats.observed_packet_count, 1);
    assert!(stats.decoded_audio_ms > 0);
    assert!(stats.non_silent_audio_ms > 0);
    assert!(stats.max_peak_amplitude > 0.0);
    assert!(stats.rms_amplitude > 0.0);
    assert_eq!(stats.first_sequence, Some(42));
    assert_eq!(stats.last_sequence, Some(42));
}

#[test]
fn audio_validator_counts_local_fast_interval_without_whole_window_ratio_gate() {
    let mut demux = WebmOpusDemux::default();
    demux.push_bytes(load_fixture_bytes("audio-itag250.webm"));
    let packets = demux.drain_packets().expect("fixture should demux");
    assert!(!packets.is_empty(), "fixture should yield opus packets");

    let mut accumulator = AudioValidationAccumulator::new();
    let start = Instant::now();
    let mut observed_at = start;
    let mut rtp_timestamp = 0u32;

    for (index, packet) in packets.iter().cycle().take(250).enumerate() {
        if index > 0 {
            let interval = match index {
                1 => Duration::from_millis(19),
                2 => Duration::from_millis(21),
                _ => Duration::from_millis(20),
            };
            observed_at += interval;
        }
        accumulator
            .observe_packet_at(
                ObservedOpusPacket {
                    sequence: index as u16,
                    timestamp: rtp_timestamp,
                    payload: packet.data.as_ref(),
                },
                observed_at,
            )
            .expect("fixture packet should decode");
        rtp_timestamp = rtp_timestamp.wrapping_add(packet.duration_samples);
    }

    let stats = accumulator.into_stats().expect("stats should be present");
    assert_eq!(stats.rtp_fast_interval_count, 1);
    assert_eq!(stats.rtp_fast_interval_min_us, 19_000);
    assert_eq!(stats.observer_anomalies.len(), 1);
    assert_eq!(stats.observer_anomalies[0].kind, "rtp_fast_interval");
    assert_eq!(
        stats.observer_anomalies[0].classification,
        "pre_pause_steady_playback"
    );
    assert_eq!(stats.decoded_audio_tempo_window_fast_count, 0);
    assert_eq!(stats.decoded_audio_tempo_window_slow_count, 0);
    assert_eq!(stats.decoded_audio_tempo_window_min_ratio_ppm, 1_000_000);
    assert_eq!(stats.decoded_audio_tempo_window_max_ratio_ppm, 1_000_000);
}

#[test]
fn audio_validator_short_window_catches_burst_that_long_window_averages_out() {
    let mut demux = WebmOpusDemux::default();
    demux.push_bytes(load_fixture_bytes("audio-itag250.webm"));
    let packets = demux.drain_packets().expect("fixture should demux");
    assert!(!packets.is_empty(), "fixture should yield opus packets");

    let mut accumulator = AudioValidationAccumulator::new();
    let start = Instant::now();
    let mut observed_at = start;
    let mut rtp_timestamp = 0u32;

    for (index, packet) in packets.iter().cycle().take(300).enumerate() {
        if index > 0 {
            let interval = if (150..175).contains(&index) {
                Duration::from_millis(18)
            } else {
                Duration::from_millis(20)
            };
            observed_at += interval;
        }
        accumulator
            .observe_packet_at(
                ObservedOpusPacket {
                    sequence: index as u16,
                    timestamp: rtp_timestamp,
                    payload: packet.data.as_ref(),
                },
                observed_at,
            )
            .expect("fixture packet should decode");
        rtp_timestamp = rtp_timestamp.wrapping_add(packet.duration_samples);
    }

    let stats = accumulator.into_stats().expect("stats should be present");
    assert_eq!(
        stats.decoded_audio_tempo_window_fast_count, 0,
        "the old 250-packet observer window should average this burst away"
    );
    assert!(
        stats.decoded_audio_short_tempo_window_fast_count > 0,
        "short observer windows should catch the burst: {stats:?}"
    );
    let fastest = stats
        .decoded_audio_short_tempo_window_fastest
        .expect("fastest short-window evidence should be recorded");
    assert_eq!(fastest.window_packet_count, 25);
    assert!(fastest.ratio_ppm > 1_020_000);
    assert_eq!(fastest.media_ms, 500);
    assert_eq!(fastest.wall_clock_us, 452_000);
    assert_eq!(fastest.first_sequence, 149);
    assert_eq!(fastest.last_sequence, 173);
    assert_eq!(fastest.classification, "pre_pause_steady_playback");
}

#[test]
fn audio_validator_excludes_controlled_pause_from_active_tempo_windows() {
    let mut demux = WebmOpusDemux::default();
    demux.push_bytes(load_fixture_bytes("audio-itag250.webm"));
    let packets = demux.drain_packets().expect("fixture should demux");
    assert!(!packets.is_empty(), "fixture should yield opus packets");

    let mut accumulator = AudioValidationAccumulator::new();
    let start = Instant::now();
    let mut observed_at = start;
    let mut rtp_timestamp = 0u32;

    for (index, packet) in packets.iter().cycle().take(260).enumerate() {
        if index > 0 {
            observed_at += Duration::from_millis(20);
        }
        accumulator
            .observe_packet_at(
                ObservedOpusPacket {
                    sequence: index as u16,
                    timestamp: rtp_timestamp,
                    payload: packet.data.as_ref(),
                },
                observed_at,
            )
            .expect("fixture packet should decode");
        rtp_timestamp = rtp_timestamp.wrapping_add(packet.duration_samples);
    }

    accumulator.reset_wall_clock_baseline_after_controlled_pause();
    observed_at += Duration::from_secs(3);

    for (index, packet) in packets.iter().cycle().take(260).enumerate() {
        if index > 0 {
            observed_at += Duration::from_millis(20);
        }
        accumulator
            .observe_packet_at(
                ObservedOpusPacket {
                    sequence: (index + 260) as u16,
                    timestamp: rtp_timestamp,
                    payload: packet.data.as_ref(),
                },
                observed_at,
            )
            .expect("fixture packet should decode");
        rtp_timestamp = rtp_timestamp.wrapping_add(packet.duration_samples);
    }

    let stats = accumulator.into_stats().expect("stats should be present");
    assert_eq!(stats.decoded_audio_ms, 10_400);
    assert_eq!(stats.wall_clock_elapsed_ms, 10_400);
    assert_eq!(stats.decoded_audio_to_wall_clock_ratio_ppm, 1_000_000);
    assert_eq!(stats.rtp_gap_count_gte_100ms, 0);
    assert_eq!(stats.decoded_audio_tempo_window_fast_count, 0);
    assert_eq!(stats.decoded_audio_tempo_window_slow_count, 0);
    assert_eq!(stats.decoded_audio_tempo_window_min_ratio_ppm, 1_000_000);
    assert_eq!(stats.decoded_audio_tempo_window_max_ratio_ppm, 1_000_000);
}

#[test]
fn audio_validator_pause_baseline_reset_preserves_pre_pause_anomaly_evidence() {
    let mut demux = WebmOpusDemux::default();
    demux.push_bytes(load_fixture_bytes("audio-itag250.webm"));
    let packets = demux.drain_packets().expect("fixture should demux");
    assert!(!packets.is_empty(), "fixture should yield opus packets");

    let mut accumulator = AudioValidationAccumulator::new();
    let start = Instant::now();
    let mut observed_at = start;
    let mut rtp_timestamp = 0u32;

    for (index, packet) in packets.iter().cycle().take(80).enumerate() {
        if index > 0 {
            let interval = match index {
                40 => Duration::from_millis(19),
                41 => Duration::from_millis(21),
                _ => Duration::from_millis(20),
            };
            observed_at += interval;
        }
        accumulator
            .observe_packet_at(
                ObservedOpusPacket {
                    sequence: index as u16,
                    timestamp: rtp_timestamp,
                    payload: packet.data.as_ref(),
                },
                observed_at,
            )
            .expect("fixture packet should decode");
        rtp_timestamp = rtp_timestamp.wrapping_add(packet.duration_samples);
    }

    accumulator.reset_wall_clock_baseline_after_controlled_pause();
    observed_at += Duration::from_secs(3);

    for (index, packet) in packets.iter().cycle().take(80).enumerate() {
        if index > 0 {
            observed_at += Duration::from_millis(20);
        }
        accumulator
            .observe_packet_at(
                ObservedOpusPacket {
                    sequence: (index + 80) as u16,
                    timestamp: rtp_timestamp,
                    payload: packet.data.as_ref(),
                },
                observed_at,
            )
            .expect("fixture packet should decode");
        rtp_timestamp = rtp_timestamp.wrapping_add(packet.duration_samples);
    }

    let stats = accumulator.into_stats().expect("stats should be present");
    assert_eq!(stats.rtp_fast_interval_count, 1);
    assert_eq!(stats.observer_anomalies.len(), 1);
    assert_eq!(stats.observer_anomalies[0].kind, "rtp_fast_interval");
    assert_eq!(
        stats.observer_anomalies[0].classification,
        "pre_pause_steady_playback"
    );
    assert_eq!(stats.decoded_audio_to_wall_clock_ratio_ppm, 1_000_000);
}

#[test]
fn audio_validator_rejects_later_fast_rolling_window_after_source_reservoir() {
    let mut demux = WebmOpusDemux::default();
    demux.push_bytes(load_fixture_bytes("audio-itag250.webm"));
    let packets = demux.drain_packets().expect("fixture should demux");
    assert!(!packets.is_empty(), "fixture should yield opus packets");

    let mut accumulator = AudioValidationAccumulator::new();
    let start = Instant::now();
    let mut observed_at = start;
    let mut rtp_timestamp = 0u32;

    for (index, packet) in packets.iter().cycle().take(620).enumerate() {
        if index > 0 {
            let interval = if index >= 320 {
                Duration::from_millis(19)
            } else {
                Duration::from_millis(20)
            };
            observed_at += interval;
        }
        accumulator
            .observe_packet_at(
                ObservedOpusPacket {
                    sequence: index as u16,
                    timestamp: rtp_timestamp,
                    payload: packet.data.as_ref(),
                },
                observed_at,
            )
            .expect("fixture packet should decode");
        rtp_timestamp = rtp_timestamp.wrapping_add(packet.duration_samples);
    }

    let stats = accumulator.into_stats().expect("stats should be present");
    assert!(stats.decoded_audio_tempo_window_post_source_buffer_count > 0);
    assert!(
        stats.decoded_audio_tempo_window_fast_count > 0,
        "later faster-than-real-time windows should be counted: {stats:?}"
    );
}

#[test]
fn audio_validator_rejects_later_slow_rolling_window_after_source_reservoir() {
    let mut demux = WebmOpusDemux::default();
    demux.push_bytes(load_fixture_bytes("audio-itag250.webm"));
    let packets = demux.drain_packets().expect("fixture should demux");
    assert!(!packets.is_empty(), "fixture should yield opus packets");

    let mut accumulator = AudioValidationAccumulator::new();
    let start = Instant::now();
    let mut observed_at = start;
    let mut rtp_timestamp = 0u32;

    for (index, packet) in packets.iter().cycle().take(620).enumerate() {
        if index > 0 {
            let interval = if index >= 320 {
                Duration::from_millis(22)
            } else {
                Duration::from_millis(20)
            };
            observed_at += interval;
        }
        accumulator
            .observe_packet_at(
                ObservedOpusPacket {
                    sequence: index as u16,
                    timestamp: rtp_timestamp,
                    payload: packet.data.as_ref(),
                },
                observed_at,
            )
            .expect("fixture packet should decode");
        rtp_timestamp = rtp_timestamp.wrapping_add(packet.duration_samples);
    }

    let stats = accumulator.into_stats().expect("stats should be present");
    assert!(stats.decoded_audio_tempo_window_post_source_buffer_count > 0);
    assert!(
        stats.decoded_audio_tempo_window_slow_count > 0,
        "later slower-than-real-time windows should be counted: {stats:?}"
    );
}
