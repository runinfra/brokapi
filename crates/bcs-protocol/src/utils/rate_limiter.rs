//! A small token-bucket rate limiter for the broker's request limits.

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

/// A token-bucket rate limiter: it lets at most `max_rps` calls per second pass, with bursts up to
/// `max_rps`.
///
/// The BCS API limits REST calls per account, about 10 per second for most services and 3 per second
/// for non-trade operations (broker docs). The HTTP client does **not** throttle on its own. Put one
/// limiter in front of the calls of a service and call [`RateLimiter::acquire`] before each call.
/// A limiter is cheap to clone, and clones share the same bucket, so one limiter can guard calls from
/// many tasks.
///
/// This limiter is for the protocol layer. It does not know the limits of the broker: you give it the
/// number. The connector crate has its own per-service limits.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use brokapi_bcs_protocol::utils::rate_limiter::RateLimiter;
///
/// let limiter = RateLimiter::new(10); // 10 calls per second
/// for _ in 0..3 {
///     limiter.acquire().await; // returns at once while the bucket has tokens
///     // client.get_order_book(...).await
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<RateLimiterInner>>,
}

#[derive(Debug)]
struct RateLimiterInner {
    max_rps: u32,
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    /// Creates a limiter that allows `max_rps` calls per second. The bucket starts full, so the first
    /// `max_rps` calls pass without waiting.
    ///
    /// `max_rps` must be above 0. With 0 the limiter would wait for a token that never comes, and
    /// [`RateLimiter::acquire`] would not return.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::utils::rate_limiter::RateLimiter;
    ///
    /// let limiter = RateLimiter::new(3);
    /// let _same_bucket = limiter.clone();
    /// ```
    pub fn new(max_rps: u32) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RateLimiterInner {
                max_rps,
                tokens: max_rps as f64,
                last_refill: Instant::now(),
            })),
        }
    }

    /// Waits until one call is allowed, then takes a token.
    ///
    /// Returns at once if the bucket has a token, otherwise sleeps for the time one token needs to
    /// grow back. Tokens grow back at `max_rps` per second. There is no timeout, and no ordering
    /// guarantee between waiting tasks.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// use brokapi_bcs_protocol::utils::rate_limiter::RateLimiter;
    /// use std::time::{Duration, Instant};
    ///
    /// let limiter = RateLimiter::new(100);
    /// let started = Instant::now();
    /// for _ in 0..100 {
    ///     limiter.acquire().await; // a full bucket: no waiting
    /// }
    /// assert!(started.elapsed() < Duration::from_millis(500));
    /// # }
    /// ```
    pub async fn acquire(&self) {
        loop {
            let sleep_dur = {
                let mut inner = self.inner.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(inner.last_refill).as_secs_f64();
                inner.tokens += elapsed * inner.max_rps as f64;
                if inner.tokens > inner.max_rps as f64 {
                    inner.tokens = inner.max_rps as f64;
                }
                inner.last_refill = now;

                if inner.tokens >= 1.0 {
                    inner.tokens -= 1.0;
                    return;
                }
                let wait = (1.0 - inner.tokens) / inner.max_rps as f64;
                Duration::from_secs_f64(wait)
            };
            tokio::time::sleep(sleep_dur).await;
        }
    }
}
