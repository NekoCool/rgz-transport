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

    #[error("node is busy: queue full on {path}")]
    NodeBusy {
        path: &'static str,
    },

    #[error("transport is not running")]
    NotRunning,

    #[error("operation rejected: {detail}")]
    Rejected {
        detail: String,
    },

    #[error("transport I/O error: recoverable={recoverable}, detail={detail}")]
    IoError {
        detail: String,
        recoverable: bool,
    },
}

pub type TransportResult<T> = Result<T, TransportError>;
