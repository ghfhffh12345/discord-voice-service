#![allow(dead_code)]

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use bytes::Bytes;

pub fn load_fixture_bytes(path: &str) -> Bytes {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path);
    Bytes::from(fs::read(workspace_root).expect("fixture should be readable"))
}

pub async fn spawn_stream_server(path: &str) -> RangeServer {
    let payload = load_fixture_bytes(path);
    spawn_test_server(ServerBehavior::HonorRange, payload).await
}

pub async fn spawn_status_server(status: &'static str) -> RangeServer {
    spawn_test_server(ServerBehavior::StaticStatus(status), Bytes::new()).await
}

async fn spawn_test_server(behavior: ServerBehavior, payload: Bytes) -> RangeServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have a local addr");
    let last_range_header = Arc::new(Mutex::new(None));
    let recorded_header = Arc::clone(&last_range_header);

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
            let (body, status, content_range, content_length) = match behavior {
                ServerBehavior::HonorRange if start > 0 => (
                    payload.get(start as usize..).unwrap_or(&[]),
                    "HTTP/1.1 206 Partial Content",
                    Some(format!(
                        "bytes {start}-{}/*",
                        payload.len().saturating_sub(1)
                    )),
                    payload.len().saturating_sub(start as usize),
                ),
                ServerBehavior::StaticStatus(status) => (&[][..], status, None, 0),
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
            let response = format!("{status}\r\n{headers}");

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
    StaticStatus(&'static str),
}

fn parse_range_start(header: &str) -> Option<u64> {
    let value = header.strip_prefix("bytes=")?;
    let (start, _) = value.split_once('-')?;
    start.parse().ok()
}
