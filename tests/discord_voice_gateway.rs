use discord_voice_service::discord_voice::rtp::RtpPacketBuilder;

#[test]
fn builds_rtp_header_for_discord_voice() {
    let packet = RtpPacketBuilder::new(7).build(1, 960, &[0xAA, 0xBB]);
    assert_eq!(
        packet,
        vec![
            0x80, 0x78, 0x00, 0x01, 0x00, 0x00, 0x03, 0xC0, 0x00, 0x00, 0x00, 0x07, 0xAA, 0xBB,
        ]
    );
}
