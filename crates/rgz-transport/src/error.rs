use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("invalid transition from {from:?} with event {event:?}")]
    InvalidTransition {
        from: crate::state::TransportState,
        event: crate::state::TransportEvent,
    },

    #[error("operation timed out")]
    Timeout,

    #[error("transport is not running")]
    NotRunning,
}

pub type TransportResult<T> = Result<T, TransportError>;
