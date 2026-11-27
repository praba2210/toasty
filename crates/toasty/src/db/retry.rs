use std::{
    fmt,
    hash::{BuildHasher, Hasher, RandomState},
    sync::Arc,
    time::Duration,
};

use crate::{Error, Result};

type OnRetry = Arc<dyn Fn(u32, &Error) + Send + Sync>;

/// How Toasty retries work that fails with an optimistic-concurrency conflict.
///
/// Backends with optimistic concurrency control, such as Aurora DSQL, abort a
/// transaction at commit when it conflicts with another one; the error is
/// [`Error::is_serialization_failure`]. A `RetryPolicy` re-runs the whole unit
/// of work: a statement outside a transaction (see
/// [`Builder::occ_retry`](super::Builder::occ_retry)) or a transaction closure
/// (see [`Db::transaction_with_retry`](super::Db::transaction_with_retry)).
/// No other error is retried.
///
/// With `d = min(base_delay * 2^(n-1), max_delay)`, the delay before retry
/// `n` is a random duration between `d / 2` and `d`. When the last retry also
/// fails, the error is [`Error::is_retries_exhausted`] and its source is the
/// last failure.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use toasty::db::RetryPolicy;
///
/// let policy = RetryPolicy::default()
///     .max_retries(5)
///     .max_delay(Duration::from_secs(2));
/// ```
#[derive(Clone)]
pub struct RetryPolicy {
    pub(crate) max_retries: u32,
    pub(crate) base_delay: Duration,
    pub(crate) max_delay: Duration,
    pub(crate) on_retry: Option<OnRetry>,
}

impl RetryPolicy {
    /// Sets how many times to retry after the first attempt. The default is 3.
    ///
    /// With 0 the work runs once and a conflict is returned as is, not
    /// wrapped in a [retries-exhausted](Error::is_retries_exhausted) error.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the delay before the first retry. The default is 50 ms.
    pub fn base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }

    /// Sets the longest delay between retries. The default is 5 seconds.
    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Calls `f` before each retry with the retry number (starting at 1) and
    /// the error that caused it.
    pub fn on_retry(mut self, f: impl Fn(u32, &Error) + Send + Sync + 'static) -> Self {
        self.on_retry = Some(Arc::new(f));
        self
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(5),
            on_retry: None,
        }
    }
}

impl RetryPolicy {
    /// Decides what follows a failed attempt, given the `retries` made so
    /// far. Returns the delay to wait before the next attempt, or the error
    /// to return: `err` itself when it is not a serialization failure or no
    /// retry is allowed, otherwise `retries_exhausted` once retries run out.
    pub(crate) fn next_retry(&self, retries: &mut u32, err: Error) -> Result<Duration> {
        if !err.is_serialization_failure() || self.max_retries == 0 {
            return Err(err);
        }
        if *retries >= self.max_retries {
            return Err(Error::retries_exhausted(retries.saturating_add(1), err));
        }
        *retries += 1;
        tracing::debug!(retry = *retries, error = %err, "retrying after serialization failure");
        if let Some(on_retry) = &self.on_retry {
            on_retry(*retries, &err);
        }
        Ok(self.delay(*retries))
    }

    /// With `d = min(base_delay * 2^(retry-1), max_delay)`, a random delay
    /// between `d / 2` and `d`.
    fn delay(&self, retry: u32) -> Duration {
        // A non-zero cap reaches `max_delay` within about 95 doublings.
        let mut cap = self.base_delay.min(self.max_delay);
        for _ in 1..retry {
            if cap.is_zero() || cap >= self.max_delay {
                break;
            }
            cap = cap.saturating_mul(2).min(self.max_delay);
        }
        let half = cap / 2;
        // A fresh `RandomState` has fresh keys, so its empty hash is random.
        let random = RandomState::new().build_hasher().finish();
        let fraction = (random >> 11) as f64 / (1u64 << 53) as f64;
        let jitter =
            Duration::try_from_secs_f64((cap - half).as_secs_f64() * fraction).unwrap_or_default();
        half.saturating_add(jitter).min(cap)
    }
}

impl fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryPolicy")
            .field("max_retries", &self.max_retries)
            .field("base_delay", &self.base_delay)
            .field("max_delay", &self.max_delay)
            .field("on_retry", &self.on_retry.as_ref().map(|_| "Fn"))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.base_delay, Duration::from_millis(50));
        assert_eq!(policy.max_delay, Duration::from_secs(5));
    }

    #[test]
    fn no_retries_returns_the_conflict_as_is() {
        let policy = RetryPolicy::default().max_retries(0);
        let err = policy
            .next_retry(&mut 0, Error::serialization_failure("conflict"))
            .unwrap_err();
        assert!(err.is_serialization_failure());
    }

    #[test]
    fn delay_grows_and_is_capped() {
        let policy = RetryPolicy::default()
            .base_delay(Duration::from_millis(100))
            .max_delay(Duration::from_millis(350));
        for _ in 0..100 {
            let first = policy.delay(1);
            assert!(first >= Duration::from_millis(50) && first <= Duration::from_millis(100));
            let second = policy.delay(2);
            assert!(second >= Duration::from_millis(100) && second <= Duration::from_millis(200));
            let capped = policy.delay(3);
            assert!(capped >= Duration::from_millis(175) && capped <= Duration::from_millis(350));
        }

        let huge = RetryPolicy::default()
            .base_delay(Duration::MAX)
            .max_delay(Duration::MAX);
        assert!(huge.delay(u32::MAX) >= Duration::MAX / 2);
    }

    #[test]
    fn delay_follows_the_formula_after_32_retries() {
        let policy = RetryPolicy::default()
            .base_delay(Duration::from_nanos(1))
            .max_delay(Duration::from_secs(10));
        let d = Duration::from_nanos(1 << 32);
        let uncapped = policy.delay(33);
        assert!(uncapped >= d / 2 && uncapped <= d);
        let capped = policy.delay(35);
        assert!(capped >= Duration::from_secs(5) && capped <= Duration::from_secs(10));
    }
}
