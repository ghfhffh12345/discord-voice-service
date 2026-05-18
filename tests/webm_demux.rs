#[path = "support/fixtures.rs"]
mod fixtures;

use bytes::Bytes;
use discord_voice_service::media::webm_demux::WebmOpusDemux;

use self::fixtures::load_fixture_bytes;

#[test]
fn demux_extracts_multiple_opus_packets_from_split_fixture() {
    let fixture = load_fixture_bytes("tests/fixtures/audio-itag250.webm");
    let split_sizes = [1usize, 2, 3, 4, 64, 128, 254, 255, 256, 257, 512, 678];

    for split_size in split_sizes {
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
    }
}
