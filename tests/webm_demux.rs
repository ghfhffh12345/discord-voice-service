#[path = "support/fixtures.rs"]
mod fixtures;

use discord_voice_service::media::webm_demux::WebmOpusDemux;

use self::fixtures::load_fixture_bytes;

#[test]
fn demux_extracts_multiple_opus_packets_from_fixture() {
    let fixture = load_fixture_bytes("tests/fixtures/audio-itag250.webm");
    let mut demux = WebmOpusDemux::default();
    demux.push_bytes(fixture);

    let packets = demux.drain_packets().unwrap();
    assert!(packets.len() > 10);
    assert!(packets.iter().all(|packet| !packet.data.is_empty()));
}
