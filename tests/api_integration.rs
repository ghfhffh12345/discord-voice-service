use discord_voice_service::api::service::map_play_request;
use discord_voice_service::proto::discordvoice::v1::PlayRequest;

#[test]
fn maps_proto_play_request_into_internal_video_id() {
    let request = PlayRequest {
        video_id: "video123".into(),
    };

    assert_eq!(map_play_request(request), "video123");
}
