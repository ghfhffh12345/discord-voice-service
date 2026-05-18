use discord_voice_service::discord_voice::rtp::RtpPacketBuilder;

#[test]
fn builds_rtp_header_for_discord_voice() {
    let packet = RtpPacketBuilder::new(7).build(1, 960, &[0xAA, 0xBB]);
    assert_eq!(packet[0], 0x80);
    assert_eq!(packet[1], 0x78);
    assert_eq!(&packet[2..4], &[0x00, 0x01]);
}
