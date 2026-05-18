#[path = "support/fixtures.rs"]
mod fixtures;

use discord_voice_service::media::http_stream::HttpOpusStream;

use self::fixtures::spawn_range_server;

#[tokio::test]
async fn http_stream_reopens_from_last_known_byte_offset() {
    let server = spawn_range_server().await;
    let mut stream = HttpOpusStream::new(server.url());

    let first = stream.read_chunk().await.unwrap().unwrap();
    assert!(!first.is_empty());

    stream.set_resume_offset(1024);
    let second = stream.read_chunk().await.unwrap().unwrap();
    assert_eq!(server.last_range_header().await, Some("bytes=1024-".into()));
    assert!(!second.is_empty());
}
