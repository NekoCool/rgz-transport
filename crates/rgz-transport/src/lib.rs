//! Async transport v2 implementation.

pub mod actor;
pub mod api;
pub mod config;
pub mod error;
pub mod metrics;
pub mod state;
pub mod transport;

pub use actor::{
    ActorChannels, DEFAULT_COMMAND_CHANNEL_CAPACITY, DEFAULT_EVENT_CHANNEL_CAPACITY,
    DEFAULT_IO_EVENT_CHANNEL_CAPACITY, DEFAULT_SUB_CMD_CHANNEL_CAPACITY, MessageHeaders,
    MessagePayload, ReplyStatus, RequestId, RequestIdGenerator, RxEvent, TxCmd, TxReply, TxRequest,
    bounded_channels, bounded_channels_with_control,
};
pub use api::{Transport, TransportHandle};
pub use config::TransportConfig;
pub use error::TransportError;
pub use metrics::{TransportMetrics, TransportMetricsSnapshot};
pub use state::{
    RecoveryPolicy, StateModel, TimedEvent, TransportEvent, TransportState, apply_events,
    transition,
};
