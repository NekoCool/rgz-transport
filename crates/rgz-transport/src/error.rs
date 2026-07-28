use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransportError {
    #[error("invalid transport state: {detail}")]
    InvalidState { detail: String },

    #[error("operation timed out")]
    Timeout,

    #[error("node is busy: queue full on {path}")]
    NodeBusy { path: &'static str },

    #[error("temporary transport error: {0}")]
    TemporaryTransport(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("service not found: {0}")]
    ServiceNotFound(String),

    #[error("internal transport error: {0}")]
    Internal(String),
}

pub type TransportResult<T> = Result<T, TransportError>;

impl TransportError {
    pub fn invalid_transition(
        from: crate::state::TransportState,
        event: crate::state::TransportEvent,
    ) -> Self {
        Self::InvalidState {
            detail: format!("invalid transition from {from:?} with event {event:?}"),
        }
    }

    pub fn not_running() -> Self {
        Self::InvalidState {
            detail: "transport is not running".to_string(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Timeout
                | Self::NodeBusy { .. }
                | Self::TemporaryTransport(_)
                | Self::ServiceNotFound(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::TransportError;

    #[test]
    fn retryable_errors_are_classified() {
        assert!(TransportError::Timeout.is_retryable());
        assert!(TransportError::NodeBusy { path: "command" }.is_retryable());
        assert!(TransportError::TemporaryTransport("retry".to_string()).is_retryable());
        assert!(TransportError::ServiceNotFound("svc".to_string()).is_retryable());
    }

    #[test]
    fn non_retryable_errors_are_classified() {
        assert!(!TransportError::InvalidState {
            detail: "bad state".to_string(),
        }
        .is_retryable());
        assert!(!TransportError::Serialization("decode".to_string()).is_retryable());
        assert!(!TransportError::Internal("panic".to_string()).is_retryable());
    }
}
