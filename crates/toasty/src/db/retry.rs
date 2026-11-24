use std::{fmt, sync::Arc, time::Duration};

use crate::Error;

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
/// The delay before retry `n` is `base_delay * 2^(n-1)`, capped at
/// `max_delay`, with random jitter. When the last retry also fails, the error
/// is [`Error::is_retries_exhausted`] and its source is the last failure.
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
#[allow(dead_code)] // Read by the F10 retry loop.
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
}
