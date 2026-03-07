//! Async transport v2 implementation.

pub mod actor;
pub mod api;
pub mod config;
pub mod error;
pub mod state;
pub mod transport;

pub use actor::{
    bounded_channels, ActorChannels, MessageHeaders, MessagePayload, ReplyStatus, RequestId,
    RequestIdGenerator, RxEvent, TxCmd, TxReply, TxRequest, DEFAULT_COMMAND_CHANNEL_CAPACITY,
    DEFAULT_EVENT_CHANNEL_CAPACITY,
};
pub use api::{Transport, TransportHandle};
pub use config::TransportConfig;
pub use error::TransportError;
pub use state::{
    RecoveryPolicy, StateModel, TimedEvent, TransportEvent, TransportState, apply_events,
    transition,
};
