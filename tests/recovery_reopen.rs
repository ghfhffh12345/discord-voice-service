#[path = "support/fixtures.rs"]
mod fixtures;

use discord_voice_service::media::http_stream::HttpOpusStream;

use self::fixtures::{spawn_non_range_server, spawn_range_server};

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

#[tokio::test]
async fn http_stream_rejects_resume_when_server_ignores_range() {
    let server = spawn_non_range_server().await;
    let mut stream = HttpOpusStream::new(server.url());

    let first = stream.read_chunk().await.unwrap().unwrap();
    assert!(!first.is_empty());

    stream.set_resume_offset(1024);
    let error = stream.read_chunk().await.unwrap_err();

    assert_eq!(server.last_range_header().await, Some("bytes=1024-".into()));
    assert!(
        error.to_string().contains("range"),
        "expected range validation error, got {error}"
    );
}
