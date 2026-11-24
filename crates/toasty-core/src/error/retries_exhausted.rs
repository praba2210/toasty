use super::Error;

/// Error when an operation still failed with a serialization failure after
/// every retry allowed by the retry policy.
///
/// The last attempt's error is available through
/// [`std::error::Error::source`].
#[derive(Debug)]
pub(super) struct RetriesExhausted {
    attempts: u32,
    pub(super) last: Error,
}

impl std::error::Error for RetriesExhausted {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.last)
    }
}

impl core::fmt::Display for RetriesExhausted {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(
            f,
            "retries exhausted after {} attempts: {}",
            self.attempts, self.last
        )
    }
}

impl Error {
    /// Creates a retries-exhausted error.
    ///
    /// Returned by optimistic-concurrency retry when the last of `attempts`
    /// attempts failed with `last`.
    ///
    /// # Examples
    ///
    /// ```
    /// use toasty_core::Error;
    ///
    /// let last = Error::serialization_failure("change conflicts with another transaction");
    /// let err = Error::retries_exhausted(4, last);
    /// assert!(err.is_retries_exhausted());
    ///
    /// let source = std::error::Error::source(&err).unwrap();
    /// assert!(source.downcast_ref::<Error>().unwrap().is_serialization_failure());
    /// ```
    pub fn retries_exhausted(attempts: u32, last: Error) -> Error {
        Error::from(super::ErrorKind::RetriesExhausted(RetriesExhausted {
            attempts,
            last,
        }))
    }

    /// Returns `true` if this error is a retries-exhausted error.
    pub fn is_retries_exhausted(&self) -> bool {
        matches!(self.kind(), super::ErrorKind::RetriesExhausted(_))
    }
}
