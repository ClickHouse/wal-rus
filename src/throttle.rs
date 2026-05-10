//! Token-bucket AsyncRead throttle
//!
//! Used to honor WALG_NETWORK_RATE_LIMIT and WALG_DISK_RATE_LIMIT
//!
//! Pacing model: bytes_read / elapsed must not exceed rate. When we'd exceed,
//! schedule a sleep until the next byte's expected wall-clock arrival

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::Sleep;

pub struct RateLimited<R> {
    inner: R,
    rate: u64,
    start: Instant,
    bytes_read: u64,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl<R> RateLimited<R> {
    /// `rate` is bytes/sec. Zero disables throttling
    pub fn new(inner: R, rate: u64) -> Self {
        Self {
            inner,
            rate,
            start: Instant::now(),
            bytes_read: 0,
            sleep: None,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for RateLimited<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.rate == 0 {
            return Pin::new(&mut self.inner).poll_read(cx, buf);
        }
        loop {
            if let Some(sleep) = self.sleep.as_mut() {
                match sleep.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        self.sleep = None;
                    }
                }
            }
            let expected_ns =
                (self.bytes_read as u128).saturating_mul(1_000_000_000) / self.rate as u128;
            let elapsed_ns = self.start.elapsed().as_nanos();
            if elapsed_ns < expected_ns {
                let delay = Duration::from_nanos((expected_ns - elapsed_ns) as u64);
                self.sleep = Some(Box::pin(tokio::time::sleep(delay)));
                continue;
            }
            let prev = buf.filled().len();
            let res = Pin::new(&mut self.inner).poll_read(cx, buf);
            if let Poll::Ready(Ok(())) = &res {
                let now = buf.filled().len();
                self.bytes_read += (now - prev) as u64;
            }
            return res;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn zero_rate_disables_throttle() {
        let payload = vec![1u8; 1024];
        let mut r = RateLimited::new(Cursor::new(payload.clone()), 0);
        let mut out = Vec::new();
        r.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, payload);
    }

    #[tokio::test]
    async fn throttle_enforces_minimum_duration() {
        // 8 KiB at 4 KiB/sec must take >= ~2 seconds wall time
        let payload = vec![1u8; 8 * 1024];
        let mut r = RateLimited::new(Cursor::new(payload), 4 * 1024);
        let start = Instant::now();
        let mut out = Vec::new();
        r.read_to_end(&mut out).await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(1800),
            "expected ~2s, got {elapsed:?}"
        );
    }
}
