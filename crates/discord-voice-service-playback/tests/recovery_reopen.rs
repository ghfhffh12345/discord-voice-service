use discord_voice_service_test_support::fixtures::{
    load_fixture_bytes, spawn_non_range_server, spawn_range_server,
    spawn_range_server_with_416_at_eof, spawn_range_server_with_initial_partial_content,
    spawn_range_server_with_partial_body_then_close,
};

use discord_voice_service_playback::media::http_stream::HttpOpusStream;

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

#[tokio::test]
async fn http_stream_treats_416_at_resume_eof_as_end_of_stream() {
    let server = spawn_range_server_with_416_at_eof().await;
    let payload_len = load_fixture_bytes("audio-itag250.webm").len() as u64 * 4;
    let mut stream = HttpOpusStream::new(server.url());

    stream.set_resume_offset(payload_len);
    let chunk = stream.read_chunk().await.unwrap();

    assert_eq!(
        server.last_range_header().await,
        Some(format!("bytes={payload_len}-"))
    );
    assert!(chunk.is_none());
}

#[tokio::test]
async fn http_stream_preserves_partial_body_before_late_eof_and_resumes() {
    let payload_len = load_fixture_bytes("audio-itag250.webm").len() * 4;
    let partial_len = payload_len / 2;
    let server = spawn_range_server_with_partial_body_then_close(partial_len).await;
    let mut stream = HttpOpusStream::new(server.url());

    let first = stream.read_chunk().await.unwrap().unwrap();

    assert_eq!(first.len(), partial_len);
    assert_eq!(stream.position().byte_offset(), partial_len as u64);

    let second = stream.read_chunk().await.unwrap().unwrap();

    assert_eq!(
        server.last_range_header().await,
        Some(format!("bytes={partial_len}-"))
    );
    assert_eq!(first.len() + second.len(), payload_len);
    assert_eq!(stream.metrics().range_reopen_count, 1);
    assert_eq!(stream.metrics().read_error_reopen_count, 1);
}

#[tokio::test]
async fn http_stream_resumes_after_initial_partial_content_segment() {
    let payload_len = load_fixture_bytes("audio-itag250.webm").len() * 4;
    let partial_len = payload_len / 2;
    let server = spawn_range_server_with_initial_partial_content(partial_len).await;
    let mut stream = HttpOpusStream::new(server.url());

    let first = stream.read_chunk().await.unwrap().unwrap();
    assert_eq!(first.len(), partial_len);
    assert_eq!(stream.position().byte_offset(), partial_len as u64);

    let second = stream
        .read_chunk()
        .await
        .unwrap()
        .expect("initial 206 segment should not be treated as whole-resource EOF");

    assert_eq!(
        server.last_range_header().await,
        Some(format!("bytes={partial_len}-"))
    );
    assert_eq!(first.len() + second.len(), payload_len);
    assert_eq!(stream.metrics().range_reopen_count, 1);
    assert_eq!(stream.metrics().read_error_reopen_count, 0);
}
