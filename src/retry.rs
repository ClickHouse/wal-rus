//! Exponential-backoff retry helper for storage / network ops
//!
//! Used by Storage backends for transient HTTP/transport failures
//! Honors WALG_DOWNLOAD_FILE_RETRIES for max attempts

use std::future::Future;
use std::time::Duration;

use aws_lc_rs::rand::{SecureRandom, SystemRandom};

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(30),
            jitter: true,
        }
    }
}

impl RetryPolicy {
    pub fn from_env() -> Self {
        let mut p = Self::default();
        if let Ok(v) = std::env::var("WALG_DOWNLOAD_FILE_RETRIES")
            && let Ok(n) = v.parse::<u32>()
            && n > 0
        {
            p.max_attempts = n;
        }
        p
    }

    fn backoff(&self, attempt: u32) -> Duration {
        // attempt is 1-based; first retry waits base_delay, then doubles
        let shift = (attempt - 1).min(20);
        let nominal = self
            .base_delay
            .saturating_mul(1u32.checked_shl(shift).unwrap_or(u32::MAX));
        let capped = nominal.min(self.max_delay);
        if self.jitter {
            // full-jitter: uniform [0, capped)
            let mut buf = [0u8; 8];
            let _ = SystemRandom::new().fill(&mut buf);
            let rand = u64::from_le_bytes(buf);
            let ms = capped.as_millis() as u64;
            if ms == 0 {
                capped
            } else {
                Duration::from_millis(rand % ms)
            }
        } else {
            capped
        }
    }
}

pub async fn with_retry<F, Fut, T, E, C>(
    policy: &RetryPolicy,
    classify: C,
    mut op: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: Fn(&E) -> bool,
    E: std::fmt::Display,
{
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt >= policy.max_attempts || !classify(&e) => return Err(e),
            Err(e) => {
                let delay = policy.backoff(attempt);
                tracing::warn!(
                    attempt,
                    max_attempts = policy.max_attempts,
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "retrying transient error"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn retries_until_success() {
        let policy = RetryPolicy {
            max_attempts: 4,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            jitter: false,
        };
        let calls = AtomicU32::new(0);
        let res: Result<u32, &str> = with_retry(
            &policy,
            |_| true,
            || async {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 { Err("flaky") } else { Ok(42) }
            },
        )
        .await;
        assert_eq!(res, Ok(42));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn stops_on_permanent() {
        let policy = RetryPolicy {
            max_attempts: 4,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            jitter: false,
        };
        let calls = AtomicU32::new(0);
        let res: Result<(), &str> = with_retry(
            &policy,
            |_| false,
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("permanent")
            },
        )
        .await;
        assert!(res.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn gives_up_after_max() {
        let policy = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            jitter: false,
        };
        let calls = AtomicU32::new(0);
        let res: Result<(), &str> = with_retry(
            &policy,
            |_| true,
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("transient")
            },
        )
        .await;
        assert!(res.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
