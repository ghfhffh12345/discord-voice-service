use bytes::Bytes;
use discord_voice_service::media::webm_demux::extract_mock_opus_packets;

#[test]
fn extracts_audio_packets_in_order_from_fixture_bytes() {
    let fixture = Bytes::from_static(b"mock-webm-opus-fixture");
    let packets = extract_mock_opus_packets(&fixture).expect("fixture should parse");
    assert_eq!(packets.len(), 3);
    assert_eq!(packets[0], Bytes::from_static(b"opus-0"));
    assert_eq!(packets[2], Bytes::from_static(b"opus-2"));
}
