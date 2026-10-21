//! A few lines of plain TCP that answer HTTP: the test sees every request and chooses every answer.

#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

/// One request the server saw: the request line, the `authorization` header.
#[derive(Debug, Clone)]
pub struct Seen {
    pub line: String,
    pub auth: String,
    pub body: String,
}

pub struct Server {
    pub url: String,
    pub seen: Arc<Mutex<Vec<Seen>>>,
}

impl Server {
    /// The bodies of the requests whose line contains `path`, in order.
    pub fn bodies(&self, path: &str) -> Vec<String> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.line.contains(path))
            .map(|s| s.body.clone())
            .collect()
    }

    pub fn count(&self, path: &str) -> usize {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.line.contains(path))
            .count()
    }
}

/// `answer(request line, authorization, n)` returns status and body. `n` counts the requests with the
/// same request line.
pub fn serve(
    answer: impl Fn(&str, &str, usize) -> (u16, String) + Send + Sync + 'static,
) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
    let log = Arc::clone(&seen);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            // Read the head and the body (Content-Length).
            loop {
                let n = stream.read(&mut chunk).unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf).to_string();
                if let Some(end) = text.find("\r\n\r\n") {
                    let want = text[..end]
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|v| v.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if buf.len() >= end + 4 + want {
                        break;
                    }
                }
            }
            let head = String::from_utf8_lossy(&buf).to_string();
            let line = head.lines().next().unwrap_or("").to_string();
            let auth = head
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .starts_with("authorization:")
                        .then(|| l.split_once(':').unwrap().1.trim().to_string())
                })
                .unwrap_or_default();
            let n = {
                let mut seen = log.lock().unwrap();
                let body = head
                    .split_once("\r\n\r\n")
                    .map(|(_, b)| b.to_string())
                    .unwrap_or_default();
                seen.push(Seen {
                    line: line.clone(),
                    auth: auth.clone(),
                    body,
                });
                seen.iter().filter(|s| s.line == line).count()
            };
            let (status, body) = answer(&line, &auth, n);
            let reply = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(reply.as_bytes());
        }
    });
    Server { url, seen }
}
