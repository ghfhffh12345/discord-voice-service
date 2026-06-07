use discord_voice_service_test_support::fixtures::load_fixture_bytes;

use bytes::Bytes;
use discord_voice_service_playback::media::webm_demux::WebmOpusDemux;

#[test]
fn demux_extracts_multiple_opus_packets_from_split_fixture() {
    let fixture = load_fixture_bytes("audio-itag250.webm");
    let split_sizes = [1usize, 2, 3, 4, 64, 128, 254, 255, 256, 257, 512, 678];
    let expected_packets = demux_fixture_in_chunks(&fixture, fixture.len());

    for split_size in split_sizes {
        let packets = demux_fixture_in_chunks(&fixture, split_size);

        assert!(
            packets.len() > 10,
            "split size {split_size} produced too few packets"
        );
        assert!(
            packets.iter().all(|packet| !packet.data.is_empty()),
            "split size {split_size} produced empty packet data"
        );
        assert!(
            packets.iter().all(|packet| packet.duration_ms > 0),
            "split size {split_size} produced zero-duration packet"
        );
        assert!(
            packets.windows(2).all(|window| {
                let left = &window[0];
                let right = &window[1];
                left.timestamp_ms <= right.timestamp_ms
            }),
            "split size {split_size} produced non-monotonic timestamps"
        );
        assert!(
            packets.windows(2).all(|window| window[0] != window[1]),
            "split size {split_size} produced duplicate adjacent packets"
        );
        assert_eq!(
            packets, expected_packets,
            "split size {split_size} diverged from one-shot demux"
        );
    }
}

#[test]
fn demux_split_stream_matches_one_shot_for_long_fixture() {
    let fixture = load_fixture_bytes("audio-long.webm");
    let expected_packets = demux_fixture_in_chunks(&fixture, fixture.len());

    for split_size in [127usize, 255, 256, 257, 511, 512, 1024, 4093] {
        let packets = demux_fixture_in_chunks(&fixture, split_size);
        assert_eq!(
            packets, expected_packets,
            "split size {split_size} diverged from one-shot demux"
        );
    }
}

fn demux_fixture_in_chunks(
    fixture: &Bytes,
    split_size: usize,
) -> Vec<discord_voice_service_playback::media::webm_demux::DemuxedPacket> {
    let mut demux = WebmOpusDemux::default();
    let mut packets = Vec::new();

    for chunk in fixture.chunks(split_size) {
        demux.push_bytes(Bytes::copy_from_slice(chunk));
        packets.extend(
            demux
                .drain_packets()
                .unwrap_or_else(|error| panic!("split size {split_size} failed: {error}")),
        );
    }

    packets
}
