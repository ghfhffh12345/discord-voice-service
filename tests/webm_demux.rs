#[path = "support/fixtures.rs"]
mod fixtures;

use bytes::Bytes;
use discord_voice_service::media::webm_demux::WebmOpusDemux;

use self::fixtures::load_fixture_bytes;

#[test]
fn demux_extracts_multiple_opus_packets_from_split_fixture() {
    let fixture = load_fixture_bytes("tests/fixtures/audio-itag250.webm");
    let mut demux = WebmOpusDemux::default();
    let mut packets = Vec::new();

    for chunk in fixture.chunks(37) {
        demux.push_bytes(Bytes::copy_from_slice(chunk));
        packets.extend(demux.drain_packets().unwrap());
    }

    assert!(packets.len() > 10);
    assert!(packets.iter().all(|packet| !packet.data.is_empty()));
    assert!(packets.iter().all(|packet| packet.duration_ms > 0));
    assert!(packets.windows(2).all(|window| {
        let left = &window[0];
        let right = &window[1];
        left.timestamp_ms <= right.timestamp_ms
    }));
}
