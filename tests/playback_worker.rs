use discord_voice_service::playback::worker::PlaybackPlan;
use discord_voice_service::ytmusic::selector::StreamFormat;

#[test]
fn playback_plan_uses_selected_itag_and_decipher_path() {
    let formats = vec![StreamFormat::new(
        250,
        "audio/webm; codecs=\"opus\"",
        70_000,
        Some(48_000),
        Some(2),
        false,
    )];

    let plan = PlaybackPlan::from_formats("video123", &formats).expect("plan should be built");
    assert_eq!(plan.video_id, "video123");
    assert_eq!(plan.selected_itag, 250);
}
