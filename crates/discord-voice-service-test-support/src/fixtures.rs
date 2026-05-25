#![allow(dead_code)]

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use bytes::Bytes;

pub fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name)
}

pub fn load_fixture_bytes(name: &str) -> Bytes {
    Bytes::from(fs::read(fixture_path(name)).expect("fixture should be readable"))
}

pub async fn spawn_stream_server(name: &str) -> RangeServer {
    let payload = load_fixture_bytes(name);
    spawn_test_server(ServerBehavior::HonorRange, payload).await
}

pub async fn spawn_stream_server_with_initial_delay(path: &str, delay: Duration) -> RangeServer {
    let payload = load_fixture_bytes(path);
    spawn_test_server(
        ServerBehavior::HonorRangeWithInitialDelay { delay },
        payload,
    )
    .await
}

pub async fn spawn_range_server() -> RangeServer {
    let payload = load_fixture_bytes("audio-itag250.webm").repeat(4);
    spawn_test_server(ServerBehavior::HonorRange, payload.into()).await
}

pub async fn spawn_non_range_server() -> RangeServer {
    let payload = load_fixture_bytes("audio-itag250.webm").repeat(4);
    spawn_test_server(ServerBehavior::IgnoreRange, payload.into()).await
}

pub async fn spawn_range_server_with_416_at_eof() -> RangeServer {
    let payload = load_fixture_bytes("audio-itag250.webm").repeat(4);
    spawn_test_server(ServerBehavior::HonorRangeWith416AtEof, payload.into()).await
}

pub async fn spawn_range_server_with_partial_body_then_close(
    bytes_before_close: usize,
) -> RangeServer {
    let payload = load_fixture_bytes("audio-itag250.webm").repeat(4);
    spawn_test_server(
        ServerBehavior::PartialBodyThenCloseOnce { bytes_before_close },
        payload.into(),
    )
    .await
}

pub async fn spawn_status_server(status: &'static str) -> RangeServer {
    spawn_test_server(ServerBehavior::StaticStatus(status), Bytes::new()).await
}

pub async fn spawn_stream_server_with_status_after_requests(
    path: &str,
    ok_requests: usize,
    status: &'static str,
) -> RangeServer {
    let payload = load_fixture_bytes(path);
    spawn_test_server(
        ServerBehavior::StaticStatusAfterRequests {
            ok_requests,
            status,
        },
        payload,
    )
    .await
}

async fn spawn_test_server(behavior: ServerBehavior, payload: Bytes) -> RangeServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have a local addr");
    let last_range_header = Arc::new(Mutex::new(None));
    let recorded_header = Arc::clone(&last_range_header);
    let request_count = Arc::new(Mutex::new(0usize));
    let request_count_state = Arc::clone(&request_count);

    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(stream) => stream,
                Err(_) => break,
            };

            let mut buffer = [0_u8; 4096];
            let read = match stream.read(&mut buffer) {
                Ok(read) if read > 0 => read,
                _ => continue,
            };
            let request = String::from_utf8_lossy(&buffer[..read]);
            let range_header = request.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("range") {
                    Some(value.trim().to_owned())
                } else {
                    None
                }
            });
            *recorded_header
                .lock()
                .expect("range header mutex should lock") = range_header.clone();

            let start = range_header
                .as_deref()
                .and_then(parse_range_start)
                .unwrap_or(0);
            let request_number = {
                let mut request_count = request_count_state
                    .lock()
                    .expect("request count mutex should lock");
                *request_count += 1;
                *request_count
            };
            let (body, status, content_range, content_length) = match behavior {
                ServerBehavior::HonorRangeWith416AtEof if start >= payload.len() as u64 => {
                    (&[][..], "HTTP/1.1 416 Range Not Satisfiable", None, 0)
                }
                ServerBehavior::HonorRange | ServerBehavior::HonorRangeWithInitialDelay { .. }
                    if start > 0 =>
                {
                    (
                        payload.get(start as usize..).unwrap_or(&[]),
                        "HTTP/1.1 206 Partial Content",
                        Some(format!(
                            "bytes {start}-{}/*",
                            payload.len().saturating_sub(1)
                        )),
                        payload.len().saturating_sub(start as usize),
                    )
                }
                ServerBehavior::PartialBodyThenCloseOnce { bytes_before_close }
                    if request_number == 1 =>
                {
                    let content_length = payload.len().saturating_sub(start as usize);
                    let end = (start as usize)
                        .saturating_add(bytes_before_close)
                        .min(payload.len());
                    (
                        payload.get(start as usize..end).unwrap_or(&[]),
                        if start > 0 {
                            "HTTP/1.1 206 Partial Content"
                        } else {
                            "HTTP/1.1 200 OK"
                        },
                        (start > 0).then(|| {
                            format!("bytes {start}-{}/*", payload.len().saturating_sub(1))
                        }),
                        content_length,
                    )
                }
                ServerBehavior::PartialBodyThenCloseOnce { .. } if start > 0 => (
                    payload.get(start as usize..).unwrap_or(&[]),
                    "HTTP/1.1 206 Partial Content",
                    Some(format!(
                        "bytes {start}-{}/*",
                        payload.len().saturating_sub(1)
                    )),
                    payload.len().saturating_sub(start as usize),
                ),
                ServerBehavior::StaticStatus(status) => (&[][..], status, None, 0),
                ServerBehavior::StaticStatusAfterRequests {
                    ok_requests,
                    status,
                } if request_number > ok_requests => (&[][..], status, None, 0),
                _ => (payload.as_ref(), "HTTP/1.1 200 OK", None, payload.len()),
            };
            let headers = if let Some(content_range) = content_range {
                format!(
                    "Content-Length: {}\r\nAccept-Ranges: bytes\r\nContent-Range: {content_range}\r\nConnection: close\r\n\r\n",
                    content_length
                )
            } else {
                format!(
                    "Content-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                    content_length
                )
            };
            let response = format!("{status}\r\n{headers}",);

            // Delay the start of every served response so playback open/retry paths
            // experience a slow first byte on each HTTP attempt.
            if let ServerBehavior::HonorRangeWithInitialDelay { delay } = behavior {
                thread::sleep(delay);
            }
            if stream.write_all(response.as_bytes()).is_err() {
                continue;
            }
            let _ = stream.write_all(body);
        }
    });

    RangeServer {
        url: format!("http://{address}"),
        last_range_header,
    }
}

pub struct RangeServer {
    url: String,
    last_range_header: Arc<Mutex<Option<String>>>,
}

impl RangeServer {
    pub fn url(&self) -> String {
        self.url.clone()
    }

    pub async fn last_range_header(&self) -> Option<String> {
        self.last_range_header
            .lock()
            .expect("range header mutex should lock")
            .clone()
    }
}

#[derive(Clone, Copy)]
enum ServerBehavior {
    HonorRange,
    HonorRangeWithInitialDelay {
        delay: Duration,
    },
    HonorRangeWith416AtEof,
    PartialBodyThenCloseOnce {
        bytes_before_close: usize,
    },
    IgnoreRange,
    StaticStatus(&'static str),
    StaticStatusAfterRequests {
        ok_requests: usize,
        status: &'static str,
    },
}

fn parse_range_start(header: &str) -> Option<u64> {
    let value = header.strip_prefix("bytes=")?;
    let (start, _) = value.split_once('-')?;
    start.parse().ok()
}
