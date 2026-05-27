use discord_voice_service_live_validation::{ObservedOpusPacket, analyze_opus_packets};
use discord_voice_service_playback::media::webm_demux::WebmOpusDemux;
use discord_voice_service_test_support::fixtures::load_fixture_bytes;

#[test]
fn audio_validator_rejects_empty_payload() {
    let error = analyze_opus_packets([ObservedOpusPacket {
        sequence: 7,
        payload: &[],
    }])
    .expect_err("empty payload should fail");

    assert!(error.to_string().contains("must not be empty"));
}

#[test]
fn audio_validator_rejects_malformed_payload() {
    let error = analyze_opus_packets([ObservedOpusPacket {
        sequence: 7,
        payload: &[0x03],
    }])
    .expect_err("malformed payload should fail");

    assert!(error.to_string().contains("unsupported opus packet header"));
}

#[test]
fn audio_validator_rejects_random_payload() {
    let error = analyze_opus_packets([ObservedOpusPacket {
        sequence: 7,
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
fn audio_validator_reports_non_silence_for_known_reference_packet() {
    let packet = [
        0x0d, 0xc5, 0xae, 0xdd, 0x5b, 0xdc, 0x3f, 0x20, 0xbe, 0x56, 0x97, 0xe5, 0x4d, 0xd1, 0xf4,
        0x37,
    ];

    let stats = analyze_opus_packets([ObservedOpusPacket {
        sequence: 42,
        payload: &packet,
    }])
    .expect("reference packet should decode");

    assert_eq!(stats.observed_packet_count, 1);
    assert!(stats.decoded_audio_ms > 0);
    assert!(stats.non_silent_audio_ms > 0);
    assert!(stats.max_peak_amplitude > 0.0);
    assert!(stats.rms_amplitude > 0.0);
    assert_eq!(stats.first_sequence, Some(42));
    assert_eq!(stats.last_sequence, Some(42));
}
