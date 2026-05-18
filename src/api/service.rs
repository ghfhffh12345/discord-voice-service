use crate::proto::discordvoice::v1::PlayRequest;

pub fn map_play_request(request: PlayRequest) -> String {
    request.video_id
}
