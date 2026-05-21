use discord_voice_service_voice::crypto::{PREFERRED_MODE, REQUIRED_MODE, pick_mode};
use discord_voice_service_voice::test_support::RtpPacketBuilder;

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

#[test]
fn prefers_aes_gcm_but_accepts_required_xchacha_mode() {
    let preferred = pick_mode(&[REQUIRED_MODE.to_owned(), PREFERRED_MODE.to_owned()]).unwrap();
    assert_eq!(preferred, PREFERRED_MODE);

    let fallback = pick_mode(&[REQUIRED_MODE.to_owned()]).unwrap();
    assert_eq!(fallback, REQUIRED_MODE);
}
