//! Deterministic write failures at the asynchronous I/O boundary.
//!
//! Wrap a TCP stream or an in-memory duplex stream before giving it to the protocol. The protocol
//! still serializes and writes real frames. [`WriteFault::fail_after`] lets a prefix through, then
//! returns `BrokenPipe` for every following write. Reads and flushes keep their normal behavior.
//!
//! # Examples
//!
//! ```
//! use brokapi_mocks::io::FaultyWrite;
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let (client, mut peer) = tokio::io::duplex(64);
//! let (mut stream, fault) = FaultyWrite::new(client);
//! fault.fail_after(3);
//! let error = stream.write_all(b"abcdef").await.unwrap_err();
//! assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
//! assert_eq!(fault.written(), 3);
//! assert_eq!(fault.failures(), 1);
//! let mut prefix = [0; 3];
//! peer.read_exact(&mut prefix).await?;
//! assert_eq!(&prefix, b"abc");
//! # Ok(())
//! # }
//! ```

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Default)]
struct State {
    remaining: Option<usize>,
    written: usize,
    failures: usize,
}

/// A handle that arms a failure and reports how many bytes and failed writes reached the boundary.
/// See the [module example](crate::io).
#[derive(Clone)]
pub struct WriteFault(Arc<Mutex<State>>);

impl WriteFault {
    /// Resets the counters. The next `bytes` bytes pass through, then writes return `BrokenPipe`.
    /// See the [module example](crate::io).
    pub fn fail_after(&self, bytes: usize) {
        *self.0.lock().expect("write fault state") = State {
            remaining: Some(bytes),
            ..State::default()
        };
    }

    /// Bytes successfully written since the last call to [`Self::fail_after`].
    /// See the [module example](crate::io).
    pub fn written(&self) -> usize {
        self.0.lock().expect("write fault state").written
    }

    /// The number of writes rejected by this fault.
    /// See the [module example](crate::io).
    pub fn failures(&self) -> usize {
        self.0.lock().expect("write fault state").failures
    }
}

/// An I/O stream with a write failure controlled by a [`WriteFault`] handle.
/// See the [module example](crate::io).
pub struct FaultyWrite<S> {
    inner: S,
    state: Arc<Mutex<State>>,
}

impl<S> FaultyWrite<S> {
    /// Starts with the fault disabled. The handle can arm it after the connection is established.
    /// See the [module example](crate::io).
    pub fn new(inner: S) -> (Self, WriteFault) {
        let state = Arc::new(Mutex::new(State::default()));
        (
            Self {
                inner,
                state: Arc::clone(&state),
            },
            WriteFault(state),
        )
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for FaultyWrite<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for FaultyWrite<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut state = this.state.lock().expect("write fault state");
        if state.remaining == Some(0) && !buf.is_empty() {
            state.failures += 1;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "simulated write failure",
            )));
        }
        let end = state.remaining.map_or(buf.len(), |n| n.min(buf.len()));
        match Pin::new(&mut this.inner).poll_write(cx, &buf[..end]) {
            Poll::Ready(Ok(n)) => {
                state.written += n;
                if let Some(left) = &mut state.remaining {
                    *left -= n;
                }
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
