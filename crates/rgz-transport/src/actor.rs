//! Internal actor message abstractions for the async runtime.
//!
//! This module defines transport-agnostic command and event types used by the v2 actor loop.
//! ZeroMQ/socket-specific payloads and API details are intentionally absent and should be
//! mapped in the adapter layer instead.

use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, sleep_until, Duration, Instant};

use crate::config::TransportConfig;
use crate::error::TransportResult;
use crate::state::{transition, TransportEvent, TransportState};
use tracing::warn;
use zeromq::{
    prelude::*,
    DealerSendHalf, DealerSocket, PubSocket, SubSocket, ZmqMessage,
};

/// Default capacity for the inbound command channel.
pub const DEFAULT_COMMAND_CHANNEL_CAPACITY: usize = 128;
/// Default capacity for the outbound event channel.
pub const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 256;
/// Default graceful shutdown timeout in milliseconds.
pub const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 10_000;
/// Default capacity for internal I/O event fan-in queue.
pub const DEFAULT_IO_EVENT_CHANNEL_CAPACITY: usize = 2048;
/// Default capacity for subscriber control queue.
pub const DEFAULT_SUB_CMD_CHANNEL_CAPACITY: usize = 512;
const IO_FRAME_PUBLISH: u8 = 0x01;
const IO_FRAME_REQUEST: u8 = 0x02;
const IO_FRAME_REPLY: u8 = 0x03;

#[derive(Debug)]
enum InternalIoEvent {
    IncomingPublish {
        topic: String,
        payload: MessagePayload,
    },
    IncomingRequest {
        request_id: RequestId,
        topic: String,
        payload: MessagePayload,
    },
    IncomingReply {
        request_id: RequestId,
        status: ReplyStatus,
        payload: MessagePayload,
    },
    IoError {
        recoverable: bool,
        detail: String,
        request_id: Option<RequestId>,
    },
}

#[derive(Debug)]
enum SubControlCommand {
    Subscribe(String),
    Unsubscribe(String),
    Stop,
}

#[derive(Clone, Debug)]
struct ActorZmqConfig {
    enabled: bool,
    io_event_capacity: usize,
    sub_cmd_capacity: usize,
    pub_bind: Option<String>,
    pub_connect: Vec<String>,
    sub_connect: Vec<String>,
    req_bind: Option<String>,
    req_connect: Vec<String>,
}

impl ActorZmqConfig {
    fn from_transport_config(config: &TransportConfig) -> Self {
        Self {
            enabled: config.enable_zeromq_io,
            io_event_capacity: config.io_event_channel_capacity,
            sub_cmd_capacity: config.sub_cmd_channel_capacity,
            pub_bind: config.zeromq_pub_bind.clone(),
            pub_connect: config.zeromq_pub_connect.clone(),
            sub_connect: config.zeromq_sub_connect.clone(),
            req_bind: config.zeromq_req_bind.clone(),
            req_connect: config.zeromq_req_connect.clone(),
        }
    }
}

struct ActorZmqRuntime {
    publisher: Option<PubSocket>,
    sub_cmd_tx: Option<mpsc::Sender<SubControlCommand>>,
    request_send: Option<DealerSendHalf>,
    io_event_rx: mpsc::Receiver<InternalIoEvent>,
    sub_task: Option<JoinHandle<()>>,
    req_task: Option<JoinHandle<()>>,
}

impl ActorZmqRuntime {
    async fn initialize(
        config: &ActorZmqConfig,
    ) -> Result<Self, String> {
        if !config.enabled {
            let (_, io_event_rx) = mpsc::channel(1);
            return Ok(Self {
                publisher: None,
                sub_cmd_tx: None,
                request_send: None,
                io_event_rx,
                sub_task: None,
                req_task: None,
            });
        }

        let mut publisher = if config.pub_bind.is_none() && config.pub_connect.is_empty() {
            None
        } else {
            Some(PubSocket::new())
        };

        if let Some(socket) = publisher.as_mut() {
            if let Some(endpoint) = &config.pub_bind {
                socket.bind(endpoint).await.map_err(|err| err.to_string())?;
            }
            for endpoint in &config.pub_connect {
                socket.connect(endpoint).await.map_err(|err| err.to_string())?;
            }
        }

        let mut subscriber = if config.sub_connect.is_empty() {
            None
        } else {
            Some(SubSocket::new())
        };
        if let Some(socket) = subscriber.as_mut() {
            for endpoint in &config.sub_connect {
                socket.connect(endpoint).await.map_err(|err| err.to_string())?;
            }
        }

        let (request_send, mut request_recv) = if config.req_bind.is_none() && config.req_connect.is_empty() {
            (None, None)
        } else {
            let mut socket = DealerSocket::new();
            if let Some(endpoint) = &config.req_bind {
                socket.bind(endpoint).await.map_err(|err| err.to_string())?;
            }
            for endpoint in &config.req_connect {
                socket.connect(endpoint).await.map_err(|err| err.to_string())?;
            }
            let (send_half, recv_half) = socket.split();
            (Some(send_half), Some(recv_half))
        };

        let (io_event_tx, io_event_rx) = mpsc::channel::<InternalIoEvent>(config.io_event_capacity);
        let mut sub_task = None;
        let mut sub_cmd_tx = None;
        let mut req_task = None;

        if let Some(mut socket) = subscriber.take() {
            let tx = io_event_tx.clone();
            let (cmd_tx, mut cmd_rx) = mpsc::channel::<SubControlCommand>(config.sub_cmd_capacity);
            sub_cmd_tx = Some(cmd_tx);
            sub_task = Some(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        cmd = cmd_rx.recv() => {
                            match cmd {
                                Some(SubControlCommand::Subscribe(topic)) => {
                                    if let Err(err) = socket.subscribe(&topic).await {
                                        try_emit_io_event(&tx, InternalIoEvent::IoError {
                                            recoverable: true,
                                            request_id: None,
                                            detail: format!("subscribe failed: {err}"),
                                        });
                                    }
                                }
                                Some(SubControlCommand::Unsubscribe(topic)) => {
                                    if let Err(err) = socket.unsubscribe(&topic).await {
                                        try_emit_io_event(&tx, InternalIoEvent::IoError {
                                            recoverable: true,
                                            request_id: None,
                                            detail: format!("unsubscribe failed: {err}"),
                                        });
                                    }
                                }
                                Some(SubControlCommand::Stop) | None => break,
                            }
                        }
                        recv = socket.recv() => {
                            match recv {
                                Ok(message) => match decode_incoming_message(message) {
                                    Ok(event) => try_emit_io_event(&tx, event),
                                    Err(detail) => {
                                        try_emit_io_event(&tx, InternalIoEvent::IoError {
                                            recoverable: true,
                                            request_id: None,
                                            detail,
                                        });
                                    }
                                },
                                Err(err) => {
                                    try_emit_io_event(&tx, InternalIoEvent::IoError {
                                        recoverable: true,
                                        request_id: None,
                                        detail: format!("subscriber recv failed: {err}"),
                                    });
                                    break;
                                }
                            }
                        }
                    }
                }
            }));
        }

        if let Some(mut socket) = request_recv.take() {
            let tx = io_event_tx.clone();
            req_task = Some(tokio::spawn(async move {
                loop {
                    match socket.recv().await {
                        Ok(message) => match decode_incoming_message(message) {
                            Ok(event) => try_emit_io_event(&tx, event),
                            Err(detail) => {
                                try_emit_io_event(&tx, InternalIoEvent::IoError {
                                    recoverable: true,
                                    request_id: None,
                                    detail,
                                });
                            }
                        },
                        Err(err) => {
                            try_emit_io_event(&tx, InternalIoEvent::IoError {
                                recoverable: true,
                                request_id: None,
                                detail: format!("request recv failed: {err}"),
                            });
                            break;
                        }
                    }
                }
            }));
        }

        Ok(Self {
            publisher,
            sub_cmd_tx,
            request_send,
            io_event_rx,
            sub_task,
            req_task,
        })
    }

    async fn stop(&mut self) {
        if let Some(tx) = self.sub_cmd_tx.take() {
            let _ = tx.try_send(SubControlCommand::Stop);
        }
        if let Some(handle) = self.sub_task.take() {
            handle.abort();
        }
        if let Some(handle) = self.req_task.take() {
            handle.abort();
        }
    }
}

fn try_emit_io_event(tx: &mpsc::Sender<InternalIoEvent>, event: InternalIoEvent) {
    match tx.try_send(event) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            warn!("dropping internal io event due to full queue");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

fn try_emit_event(tx: &mpsc::Sender<RxEvent>, event: RxEvent) {
    match tx.try_send(event) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            warn!("dropping actor event due to full queue");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

async fn emit_state_error(
    state: &Arc<Mutex<TransportState>>,
    event_tx: &mpsc::Sender<RxEvent>,
    recoverable: bool,
    request_id: Option<RequestId>,
    detail: &str,
) {
    let event = if recoverable {
        TransportEvent::RecoverableError
    } else {
        TransportEvent::FatalError
    };
    if let Ok(mut current) = state.try_lock() {
        let _ = transition(*current, event).map(|next| {
            *current = next;
        });
    } else {
        let mut current = state.lock().await;
        let _ = transition(*current, event).map(|next| {
            *current = next;
        });
    }

    try_emit_event(
        event_tx,
        RxEvent::Error {
            request_id,
            status: ReplyStatus::Error,
            detail: if recoverable {
                format!("recoverable I/O error: {detail}")
            } else {
                format!("fatal I/O error: {detail}")
            },
        },
    );
}

async fn emit_transport_recovery(state: &Arc<Mutex<TransportState>>) {
    let mut current = state.lock().await;
    let _ = transition(*current, TransportEvent::RetrySucceeded).map(|next| {
        *current = next;
    });
}

fn encode_u32_len_prefix(len: usize) -> Result<[u8; 4], &'static str> {
    u32::try_from(len).map(|value| value.to_le_bytes()).map_err(|_| "frame exceeds u32 length")
}

fn encode_u64_value(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

fn status_to_byte(status: &ReplyStatus) -> u8 {
    match status {
        ReplyStatus::Ok => 0,
        ReplyStatus::Rejected => 1,
        ReplyStatus::NotFound => 2,
        ReplyStatus::Timeout => 3,
        ReplyStatus::Error => 4,
    }
}

fn byte_to_status(value: u8) -> ReplyStatus {
    match value {
        0 => ReplyStatus::Ok,
        1 => ReplyStatus::Rejected,
        2 => ReplyStatus::NotFound,
        3 => ReplyStatus::Timeout,
        _ => ReplyStatus::Error,
    }
}

#[cfg(test)]
fn encode_publish_frame(topic: &str, payload: &[u8]) -> Result<Vec<u8>, &'static str> {
    let topic_len = encode_u32_len_prefix(topic.len())?;
    let payload_len = encode_u32_len_prefix(payload.len())?;

    let mut frame = Vec::with_capacity(1 + 4 + topic.len() + 4 + payload.len());
    frame.push(IO_FRAME_PUBLISH);
    frame.extend_from_slice(&topic_len);
    frame.extend_from_slice(topic.as_bytes());
    frame.extend_from_slice(&payload_len);
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn encode_publish_message(topic: &str, payload: &[u8]) -> Result<ZmqMessage, &'static str> {
    if topic.is_empty() {
        return Err("publish topic is empty");
    }
    let mut message: ZmqMessage = topic.to_string().into();
    message.push_back(payload.to_vec().into());
    Ok(message)
}

fn encode_request_frame(request: &TxRequest) -> Result<Vec<u8>, &'static str> {
    let topic_len = encode_u32_len_prefix(request.topic.len())?;
    let payload_len = encode_u32_len_prefix(request.payload.len())?;

    let mut frame = Vec::with_capacity(1 + 8 + 4 + request.topic.len() + 4 + request.payload.len());
    frame.push(IO_FRAME_REQUEST);
    frame.extend_from_slice(&encode_u64_value(request.request_id));
    frame.extend_from_slice(&topic_len);
    frame.extend_from_slice(request.topic.as_bytes());
    frame.extend_from_slice(&payload_len);
    frame.extend_from_slice(&request.payload);
    Ok(frame)
}

fn encode_reply_frame(reply: &TxReply) -> Result<Vec<u8>, &'static str> {
    let payload_len = encode_u32_len_prefix(reply.payload.len())?;

    let mut frame = Vec::with_capacity(1 + 8 + 1 + 4 + reply.payload.len());
    frame.push(IO_FRAME_REPLY);
    frame.extend_from_slice(&encode_u64_value(reply.request_id));
    frame.push(status_to_byte(&reply.status));
    frame.extend_from_slice(&payload_len);
    frame.extend_from_slice(&reply.payload);
    Ok(frame)
}

fn decode_u32_frame(data: &[u8], cursor: &mut usize) -> Option<u32> {
    if data.len().saturating_sub(*cursor) < 4 {
        return None;
    }
    let value = u32::from_le_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
    ]);
    *cursor += 4;
    Some(value)
}

fn decode_u64_frame(data: &[u8], cursor: &mut usize) -> Option<u64> {
    if data.len().saturating_sub(*cursor) < 8 {
        return None;
    }
    let value = u64::from_le_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
        data[*cursor + 4],
        data[*cursor + 5],
        data[*cursor + 6],
        data[*cursor + 7],
    ]);
    *cursor += 8;
    Some(value)
}

fn decode_incoming_message(message: ZmqMessage) -> Result<InternalIoEvent, String> {
    if message.len() >= 2 {
        let topic_frame = message
            .get(0)
            .ok_or_else(|| "missing publish topic frame".to_string())?;
        if let Ok(topic) = String::from_utf8(topic_frame.to_vec())
        {
            let payload = message
                .get(1)
                .ok_or_else(|| "missing publish payload frame".to_string())?
                .to_vec();
            return Ok(InternalIoEvent::IncomingPublish { topic, payload });
        }
    }

    let data: Vec<u8> = message
        .try_into()
        .map_err(|err: &str| format!("decode zmq message failed: {err}"))?;
    if data.is_empty() {
        return Err("empty zmq message".to_string());
    }

    let mut cursor = 0usize;
    match data[0] {
        IO_FRAME_PUBLISH => {
            cursor += 1;
            let topic_len = decode_u32_frame(&data, &mut cursor).ok_or("invalid publish frame")?;
            let topic_end = cursor + topic_len as usize;
            if topic_end > data.len() {
                return Err("invalid publish topic length".to_string());
            }
            let topic = String::from_utf8(data[cursor..topic_end].to_vec())
                .map_err(|_| "invalid publish topic utf8".to_string())?;
            cursor = topic_end;
            let payload_len = decode_u32_frame(&data, &mut cursor).ok_or("invalid publish payload length")?;
            let payload_end = cursor + payload_len as usize;
            if payload_end > data.len() {
                return Err("invalid publish payload length".to_string());
            }
            let payload = data[cursor..payload_end].to_vec();
            Ok(InternalIoEvent::IncomingPublish { topic, payload })
        }
        IO_FRAME_REQUEST => {
            cursor += 1;
            let request_id = decode_u64_frame(&data, &mut cursor).ok_or("invalid request id")?;
            let topic_len = decode_u32_frame(&data, &mut cursor).ok_or("invalid request topic length")?;
            let topic_end = cursor + topic_len as usize;
            if topic_end > data.len() {
                return Err("invalid request topic length".to_string());
            }
            let topic = String::from_utf8(data[cursor..topic_end].to_vec())
                .map_err(|_| "invalid request topic utf8".to_string())?;
            cursor = topic_end;
            let payload_len = decode_u32_frame(&data, &mut cursor).ok_or("invalid request payload length")?;
            let payload_end = cursor + payload_len as usize;
            if payload_end > data.len() {
                return Err("invalid request payload length".to_string());
            }
            let payload = data[cursor..payload_end].to_vec();
            Ok(InternalIoEvent::IncomingRequest {
                request_id,
                topic,
                payload,
            })
        }
        IO_FRAME_REPLY => {
            cursor += 1;
            let request_id = decode_u64_frame(&data, &mut cursor).ok_or("invalid reply id")?;
            if data.len().saturating_sub(cursor) < 1 {
                return Err("invalid reply status".to_string());
            }
            let status = byte_to_status(data[cursor]);
            cursor += 1;
            let payload_len = decode_u32_frame(&data, &mut cursor).ok_or("invalid reply payload length")?;
            let payload_end = cursor + payload_len as usize;
            if payload_end > data.len() {
                return Err("invalid reply payload length".to_string());
            }
            let payload = data[cursor..payload_end].to_vec();
            Ok(InternalIoEvent::IncomingReply {
                request_id,
                status,
                payload,
            })
        }
        _ => Err("unknown zmq message frame".to_string()),
    }
}

/// Identifier used to correlate request/response messages.
pub type RequestId = u64;

/// Generic transport payload in bytes.
pub type MessagePayload = Vec<u8>;

/// Optional metadata carried with command/event payloads.
pub type MessageHeaders = BTreeMap<String, String>;

/// Error classes for reply/event status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReplyStatus {
    /// Successful processing.
    Ok,
    /// Rejection by local actor policy or validation.
    Rejected,
    /// No route/target for the logical address.
    NotFound,
    /// Request processing timed out.
    Timeout,
    /// Transport/internal failure.
    Error,
}

/// Payload for async request messages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxRequest {
    /// Correlation identifier for request/response matching.
    pub request_id: RequestId,
    /// Logical destination topic/channel.
    pub topic: String,
    /// Request body.
    pub payload: MessagePayload,
    /// Optional request-scoped metadata.
    pub headers: Option<MessageHeaders>,
    /// Optional request timeout in milliseconds.
    pub timeout_ms: Option<u64>,
}

/// Payload for async reply messages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxReply {
    /// Correlation identifier for request/response matching.
    pub request_id: RequestId,
    /// Reply body.
    pub payload: MessagePayload,
    /// Response status.
    pub status: ReplyStatus,
    /// Optional reply-scoped metadata.
    pub headers: Option<MessageHeaders>,
}

/// Commands sent into the actor loop (inbound stream).
#[derive(Debug)]
pub enum TxCmd {
    /// Publish a message to one or more subscribers.
    Publish {
        /// Destination topic.
        topic: String,
        /// Message body.
        payload: MessagePayload,
        /// Optional metadata.
        headers: Option<MessageHeaders>,
    },

    /// Send request that must receive a reply tied to `request_id`.
    SendRequest {
        /// Request envelope.
        request: TxRequest,
    },

    /// Send an explicit reply to a previously issued request.
    SendReply {
        /// Reply envelope.
        reply: TxReply,
    },

    /// Register subscription for inbound messages.
    Subscribe {
        /// Topic to subscribe.
        topic: String,
    },

    /// Unregister subscription.
    Unsubscribe {
        /// Topic to unsubscribe.
        topic: String,
    },

    /// Connect to an adapter endpoint.
    Connect {
        /// Endpoint string for the adapter.
        endpoint: String,
        /// Optional namespace or transport profile.
        namespace: Option<String>,
    },

    /// Disconnect from a connected endpoint.
    Disconnect {
        /// Endpoint string to disconnect from.
        endpoint: String,
        /// Optional reason.
        reason: Option<String>,
    },

    /// Request to terminate actor runtime.
    Shutdown {
        /// If true, actor blocks shutdown until outstanding requests are settled or timeout.
        /// If false, actor immediately transitions and cancels pending requests.
        graceful: bool,
        /// Optional graceful shutdown timeout in milliseconds.
        /// Applies only when graceful is true. None uses `DEFAULT_SHUTDOWN_TIMEOUT_MS`.
        timeout_ms: Option<u64>,
        /// Acknowledgement channel for shutdown completion.
        ack: oneshot::Sender<TransportResult<()>>,
    },
}

/// Events produced by the actor loop (outbound stream).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RxEvent {
    /// An incoming publish event was received.
    IncomingPublish {
        /// Source topic.
        topic: String,
        /// Message body.
        payload: MessagePayload,
        /// Optional metadata.
        headers: Option<MessageHeaders>,
    },

    /// An incoming request from remote side.
    IncomingRequest {
        /// Correlation identifier.
        request_id: RequestId,
        /// Requested topic.
        topic: String,
        /// Request body.
        payload: MessagePayload,
        /// Optional metadata.
        headers: Option<MessageHeaders>,
    },

    /// Reply/event for a previously issued request.
    IncomingReply {
        /// Correlation identifier.
        request_id: RequestId,
        /// Reply body.
        payload: MessagePayload,
        /// Reply status.
        status: ReplyStatus,
        /// Optional metadata.
        headers: Option<MessageHeaders>,
    },

    /// Subscription state changed.
    Subscribed {
        /// Topic subscribed.
        topic: String,
    },

    /// Subscription state changed.
    Unsubscribed {
        /// Topic unsubscribed.
        topic: String,
    },

    /// Endpoint is now connected.
    Connected {
        /// Connected endpoint.
        endpoint: String,
    },

    /// Endpoint disconnected.
    Disconnected {
        /// Disconnected endpoint.
        endpoint: String,
        /// Optional reason.
        reason: Option<String>,
    },

    /// Error related to a correlation id or stream control.
    Error {
        /// Optional correlated request id.
        request_id: Option<RequestId>,
        /// Error classification.
        status: ReplyStatus,
        /// Human-readable description.
        detail: String,
    },

    /// Shutdown request has been observed by actor.
    ShutdownRequested,
    /// Shutdown cycle completed.
    ShutdownCompleted,
}

/// Runtime-owned channels for interacting with the actor.
pub struct ActorChannels {
    /// Inbound command sender (external -> actor).
    pub command_tx: mpsc::Sender<TxCmd>,
    /// Inbound command receiver (actor side).
    pub command_rx: mpsc::Receiver<TxCmd>,
    /// Inbound control sender (shutdown).
    pub control_tx: mpsc::Sender<TxCmd>,
    /// Inbound control receiver (actor side).
    pub control_rx: mpsc::Receiver<TxCmd>,
    /// Outbound event sender (actor -> external).
    pub event_tx: mpsc::Sender<RxEvent>,
    /// Outbound event receiver (external side).
    pub event_rx: mpsc::Receiver<RxEvent>,
}

/// Generates monotonically increasing request ids for request/response matching.
pub struct RequestIdGenerator {
    counter: AtomicU64,
}

impl Default for RequestIdGenerator {
    fn default() -> Self {
        Self {
            counter: AtomicU64::new(1),
        }
    }
}

impl RequestIdGenerator {
    /// Create a new request id generator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a new request id for outbound request correlation.
    pub fn next(&self) -> RequestId {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}

/// Create bounded command and event channels for actor integration.
pub fn bounded_channels(
    command_capacity: usize,
    event_capacity: usize,
) -> ActorChannels {
    bounded_channels_with_control(command_capacity, command_capacity, event_capacity)
}

/// Create bounded channels with explicit command/control capacities.
pub fn bounded_channels_with_control(
    command_capacity: usize,
    control_capacity: usize,
    event_capacity: usize,
) -> ActorChannels {
    let (command_tx, command_rx) = mpsc::channel(command_capacity);
    let (control_tx, control_rx) = mpsc::channel(control_capacity);
    let (event_tx, event_rx) = mpsc::channel(event_capacity);
    ActorChannels {
        command_tx,
        command_rx,
        control_tx,
        control_rx,
        event_tx,
        event_rx,
    }
}

impl Default for ActorChannels {
    fn default() -> Self {
        bounded_channels(DEFAULT_COMMAND_CHANNEL_CAPACITY, DEFAULT_EVENT_CHANNEL_CAPACITY)
    }
}

/// Actor abstraction for future async runtime ownership.
pub struct TransportActor;

impl TransportActor {
    pub async fn run_with_channels(
        command_rx: mpsc::Receiver<TxCmd>,
        control_rx: mpsc::Receiver<TxCmd>,
        event_tx: mpsc::Sender<RxEvent>,
    ) {
        let state = Arc::new(Mutex::new(TransportState::Running));
        Self::run_with_channels_with_config(
            command_rx,
            control_rx,
            event_tx,
            state,
            TransportConfig::default(),
        )
        .await;
    }

    pub async fn run_with_channels_with_config(
        command_rx: mpsc::Receiver<TxCmd>,
        control_rx: mpsc::Receiver<TxCmd>,
        event_tx: mpsc::Sender<RxEvent>,
        state: Arc<Mutex<TransportState>>,
        config: TransportConfig,
    ) {
        let io_config = ActorZmqConfig::from_transport_config(&config);
        let io_runtime = match ActorZmqRuntime::initialize(&io_config).await {
            Ok(runtime) => runtime,
            Err(err) => {
                emit_state_error(
                    &state,
                    &event_tx,
                    false,
                    None,
                    &format!("zeromq initialization failed: {err}"),
                )
                .await;
                ActorZmqRuntime::initialize(&ActorZmqConfig {
                    enabled: false,
                    io_event_capacity: config.io_event_channel_capacity,
                    sub_cmd_capacity: config.sub_cmd_channel_capacity,
                    pub_bind: None,
                    pub_connect: Vec::new(),
                    sub_connect: Vec::new(),
                    req_bind: None,
                    req_connect: Vec::new(),
                })
                .await
                .expect("failed fallback zeromq runtime init")
            }
        };
        Self::run_loop(command_rx, control_rx, event_tx, state, io_runtime).await;
    }

    async fn run_loop(
        mut command_rx: mpsc::Receiver<TxCmd>,
        mut control_rx: mpsc::Receiver<TxCmd>,
        event_tx: mpsc::Sender<RxEvent>,
        state: Arc<Mutex<TransportState>>,
        mut io_runtime: ActorZmqRuntime,
    ) {
        let pending_requests: Arc<Mutex<HashMap<RequestId, Option<u64>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let inbound_requests: Arc<Mutex<HashSet<RequestId>>> =
            Arc::new(Mutex::new(HashSet::new()));
        let mut command_open = true;
        let mut control_open = true;
        let io_runtime_active =
            io_runtime.request_send.is_some() || io_runtime.sub_cmd_tx.is_some() || io_runtime.publisher.is_some();

        while command_open || control_open {
            tokio::select! {
                biased;
                io_event = io_runtime.io_event_rx.recv(), if io_runtime_active => {
                    match io_event {
                        Some(InternalIoEvent::IncomingPublish { topic, payload }) => {
                            try_emit_event(
                                &event_tx,
                                RxEvent::IncomingPublish {
                                    topic,
                                    payload,
                                    headers: None,
                                },
                            );
                            emit_transport_recovery(&state).await;
                        }
                        Some(InternalIoEvent::IncomingRequest {
                            request_id,
                            topic,
                            payload,
                        }) => {
                            let mut inbound = inbound_requests.lock().await;
                            let _ = inbound.insert(request_id);
                            try_emit_event(
                                &event_tx,
                                RxEvent::IncomingRequest {
                                    request_id,
                                    topic,
                                    payload,
                                    headers: None,
                                },
                            );
                            emit_transport_recovery(&state).await;
                        }
                        Some(InternalIoEvent::IncomingReply {
                            request_id,
                            status,
                            payload,
                        }) => {
                            let mut pending = pending_requests.lock().await;
                            if pending.remove(&request_id).is_some() {
                                try_emit_event(
                                    &event_tx,
                                    RxEvent::IncomingReply {
                                        request_id,
                                        payload,
                                        status,
                                        headers: None,
                                    },
                                );
                                emit_transport_recovery(&state).await;
                            } else {
                                try_emit_event(
                                    &event_tx,
                                    RxEvent::Error {
                                        request_id: Some(request_id),
                                        status: ReplyStatus::Error,
                                        detail: "unexpected reply id".to_string(),
                                    },
                                );
                            }
                        }
                        Some(InternalIoEvent::IoError {
                            recoverable,
                            request_id,
                            detail,
                        }) => {
                            emit_state_error(
                                &state,
                                &event_tx,
                                recoverable,
                                request_id,
                                &detail,
                            )
                            .await;
                        }
                        None => {}
                    }
                }
                cmd = control_rx.recv(), if control_open => {
                    match cmd {
                        Some(TxCmd::Shutdown {
                            graceful,
                            timeout_ms,
                            ack,
                        }) => {
                            try_emit_event(&event_tx, RxEvent::ShutdownRequested);
                            let mut pending = pending_requests.lock().await;
                            let shutdown_timeout_ms = timeout_ms.unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT_MS);
                            let result = shutdown_wait(
                                graceful,
                                shutdown_timeout_ms,
                                &mut command_rx,
                                &mut control_rx,
                                &mut pending,
                                &event_tx,
                            )
                            .await;
                            try_emit_event(&event_tx, RxEvent::ShutdownCompleted);
                            let _ = ack.send(result);
                            io_runtime.stop().await;
                            return;
                        }
                        Some(_) => {
                            try_emit_event(
                                &event_tx,
                                RxEvent::Error {
                                    request_id: None,
                                    status: ReplyStatus::Rejected,
                                    detail: "non-shutdown command sent on control channel".to_string(),
                                },
                            );
                        }
                        None => {
                            control_open = false;
                        }
                    }
                }
                cmd = command_rx.recv(), if command_open => {
                    match cmd {
                        Some(TxCmd::Publish {
                            topic,
                            payload,
                            headers,
                        }) => {
                            let mut sent = false;
                            if let Some(publisher) = io_runtime.publisher.as_mut() {
                                match encode_publish_message(&topic, &payload) {
                                    Ok(message) => match publisher.send(message).await {
                                        Ok(_) => {
                                            emit_transport_recovery(&state).await;
                                            sent = true;
                                        }
                                        Err(err) => {
                                            emit_state_error(
                                                &state,
                                                &event_tx,
                                                true,
                                                None,
                                                &format!("publish send failed: {err}"),
                                            )
                                            .await;
                                        }
                                    },
                                    Err(err) => {
                                        emit_state_error(
                                            &state,
                                            &event_tx,
                                            true,
                                            None,
                                            err,
                                        )
                                        .await;
                                    }
                                }
                            } else {
                                sent = true;
                            }

                            if !sent {
                                continue;
                            }
                            if io_runtime.publisher.is_none() {
                                try_emit_event(
                                    &event_tx,
                                    RxEvent::IncomingPublish {
                                        topic,
                                        payload,
                                        headers,
                                    },
                                );
                            }
                        }
                        Some(TxCmd::SendRequest { request }) => {
                            let timeout_ms = request.timeout_ms;
                            let request_id = request.request_id;
                            {
                                let mut pending = pending_requests.lock().await;
                                let _ = pending.insert(request_id, timeout_ms);
                            }

                            if let Some(timeout_ms) = timeout_ms {
                                let pending_requests = Arc::clone(&pending_requests);
                                let timeout_tx = event_tx.clone();
                                tokio::spawn(async move {
                                    sleep(Duration::from_millis(timeout_ms)).await;
                                    let mut pending = pending_requests.lock().await;
                                    if pending.remove(&request_id).is_some() {
                                        try_emit_event(
                                            &timeout_tx,
                                            RxEvent::Error {
                                                request_id: Some(request_id),
                                                status: ReplyStatus::Timeout,
                                                detail: "request timed out".to_string(),
                                            },
                                        );
                                    }
                                });
                            }

                            let mut dispatched = false;
                            if let Some(request_send) = io_runtime.request_send.as_mut() {
                                match encode_request_frame(&request) {
                                    Ok(frame) => {
                                        let message: ZmqMessage = frame.into();
                                        match request_send.send(message).await {
                                            Ok(_) => {
                                                emit_transport_recovery(&state).await;
                                                dispatched = true;
                                            }
                                            Err(err) => {
                                                let mut pending = pending_requests.lock().await;
                                                pending.remove(&request_id);
                                                try_emit_event(
                                                    &event_tx,
                                                    RxEvent::Error {
                                                        request_id: Some(request_id),
                                                        status: ReplyStatus::Error,
                                                        detail: format!("request send failed: {err}"),
                                                    },
                                                );
                                                emit_state_error(
                                                    &state,
                                                    &event_tx,
                                                    true,
                                                    Some(request_id),
                                                    &format!("request send failed: {err}"),
                                                )
                                                .await;
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        let mut pending = pending_requests.lock().await;
                                        pending.remove(&request_id);
                                        try_emit_event(
                                            &event_tx,
                                            RxEvent::Error {
                                                request_id: Some(request_id),
                                                status: ReplyStatus::Error,
                                                detail: err.to_string(),
                                            },
                                        );
                                        emit_state_error(
                                            &state,
                                            &event_tx,
                                            true,
                                            Some(request_id),
                                            err,
                                        )
                                        .await;
                                    }
                                }
                            } else {
                                dispatched = true;
                            }

                            if !dispatched {
                                continue;
                            }
                            if io_runtime.request_send.is_none() {
                                try_emit_event(
                                    &event_tx,
                                    RxEvent::IncomingRequest {
                                        request_id,
                                        topic: request.topic,
                                        payload: request.payload,
                                        headers: request.headers,
                                    },
                                );
                            }
                        }
                        Some(TxCmd::SendReply { reply }) => {
                            if let Some(request_send) = io_runtime.request_send.as_mut() {
                                let is_outbound = {
                                    let mut pending = pending_requests.lock().await;
                                    pending.remove(&reply.request_id).is_some()
                                };
                                let is_inbound = if is_outbound {
                                    false
                                } else {
                                    let mut inbound = inbound_requests.lock().await;
                                    inbound.remove(&reply.request_id)
                                };

                                match encode_reply_frame(&reply) {
                                    Ok(frame) => {
                                        let message: ZmqMessage = frame.into();
                                        if is_outbound || is_inbound {
                                            match request_send.send(message).await {
                                                Ok(_) => {
                                                    emit_transport_recovery(&state).await;
                                                    if is_outbound {
                                                        try_emit_event(
                                                            &event_tx,
                                                            RxEvent::IncomingReply {
                                                                request_id: reply.request_id,
                                                                payload: reply.payload,
                                                                status: reply.status,
                                                                headers: reply.headers,
                                                            },
                                                        );
                                                    }
                                                }
                                                Err(err) => {
                                                    try_emit_event(
                                                        &event_tx,
                                                        RxEvent::Error {
                                                            request_id: Some(reply.request_id),
                                                            status: ReplyStatus::Error,
                                                            detail: format!("reply send failed: {err}"),
                                                        },
                                                    );
                                                    emit_state_error(
                                                        &state,
                                                        &event_tx,
                                                        true,
                                                        Some(reply.request_id),
                                                        &format!("reply send failed: {err}"),
                                                    )
                                                    .await;
                                                }
                                            }
                                        } else {
                                            try_emit_event(
                                                &event_tx,
                                                RxEvent::Error {
                                                    request_id: Some(reply.request_id),
                                                    status: ReplyStatus::Error,
                                                    detail: "unexpected reply id".to_string(),
                                                },
                                            );
                                        }
                                    }
                                    Err(err) => {
                                        try_emit_event(
                                            &event_tx,
                                            RxEvent::Error {
                                                request_id: Some(reply.request_id),
                                                status: ReplyStatus::Error,
                                                detail: err.to_string(),
                                            },
                                        );
                                        emit_state_error(
                                            &state,
                                            &event_tx,
                                            true,
                                            Some(reply.request_id),
                                            err,
                                        )
                                        .await;
                                    }
                                }
                            } else if {
                                let mut pending = pending_requests.lock().await;
                                pending.remove(&reply.request_id).is_some()
                            } {
                                try_emit_event(
                                    &event_tx,
                                    RxEvent::IncomingReply {
                                        request_id: reply.request_id,
                                        payload: reply.payload,
                                        status: reply.status,
                                        headers: reply.headers,
                                    },
                                );
                                emit_transport_recovery(&state).await;
                            } else {
                                try_emit_event(
                                    &event_tx,
                                    RxEvent::Error {
                                        request_id: Some(reply.request_id),
                                        status: ReplyStatus::Error,
                                        detail: "unexpected reply id".to_string(),
                                    },
                                );
                            }
                        }
                        Some(TxCmd::Subscribe { topic }) => {
                            if let Some(sub_cmd_tx) = io_runtime.sub_cmd_tx.as_ref() {
                                if let Err(err) = sub_cmd_tx.try_send(SubControlCommand::Subscribe(topic.clone())) {
                                    try_emit_event(
                                        &event_tx,
                                        RxEvent::Error {
                                            request_id: None,
                                            status: ReplyStatus::Error,
                                            detail: format!("subscribe command failed: {err}"),
                                        },
                                    );
                                    emit_state_error(
                                        &state,
                                        &event_tx,
                                        true,
                                        None,
                                        &format!("subscribe command failed: {err}"),
                                    )
                                    .await;
                                } else {
                                    emit_transport_recovery(&state).await;
                                    try_emit_event(&event_tx, RxEvent::Subscribed { topic });
                                }
                            } else {
                                try_emit_event(&event_tx, RxEvent::Subscribed { topic });
                            }
                        }
                        Some(TxCmd::Unsubscribe { topic }) => {
                            if let Some(sub_cmd_tx) = io_runtime.sub_cmd_tx.as_ref() {
                                if let Err(err) = sub_cmd_tx.try_send(SubControlCommand::Unsubscribe(topic.clone())) {
                                    try_emit_event(
                                        &event_tx,
                                        RxEvent::Error {
                                            request_id: None,
                                            status: ReplyStatus::Error,
                                            detail: format!("unsubscribe command failed: {err}"),
                                        },
                                    );
                                    emit_state_error(
                                        &state,
                                        &event_tx,
                                        true,
                                        None,
                                        &format!("unsubscribe command failed: {err}"),
                                    )
                                    .await;
                                } else {
                                    emit_transport_recovery(&state).await;
                                    try_emit_event(&event_tx, RxEvent::Unsubscribed { topic });
                                }
                            } else {
                                try_emit_event(&event_tx, RxEvent::Unsubscribed { topic });
                            }
                        }
                        Some(TxCmd::Connect { endpoint, namespace: _ }) => {
                            try_emit_event(&event_tx, RxEvent::Connected { endpoint });
                            emit_transport_recovery(&state).await;
                        }
                        Some(TxCmd::Disconnect { endpoint, reason }) => {
                            try_emit_event(
                                &event_tx,
                                RxEvent::Disconnected {
                                    endpoint,
                                    reason,
                                },
                            );
                        }
                        Some(TxCmd::Shutdown { .. }) => {
                            try_emit_event(
                                &event_tx,
                                RxEvent::Error {
                                    request_id: None,
                                    status: ReplyStatus::Rejected,
                                    detail: "shutdown command sent on normal channel".to_string(),
                                },
                            );
                        }
                        None => {
                            command_open = false;
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }
    }
}

async fn shutdown_wait(
    graceful: bool,
    timeout_ms: u64,
    command_rx: &mut mpsc::Receiver<TxCmd>,
    control_rx: &mut mpsc::Receiver<TxCmd>,
    pending_requests: &mut HashMap<RequestId, Option<u64>>,
    event_tx: &mpsc::Sender<RxEvent>,
) -> TransportResult<()> {
    if !graceful {
        for request_id in pending_requests.drain().map(|(request_id, _)| request_id) {
            try_emit_event(
                event_tx,
                RxEvent::Error {
                    request_id: Some(request_id),
                    status: ReplyStatus::Error,
                    detail: "actor shutting down".to_string(),
                },
            );
        }
        return Ok(());
    }

    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if pending_requests.is_empty() {
            return Ok(());
        }

        tokio::select! {
            biased;
            cmd = control_rx.recv() => {
                match cmd {
                    Some(TxCmd::Shutdown { ack, .. }) => {
                        let _ = ack.send(Ok(()));
                    }
                    Some(_) => {
                        try_emit_event(
                            event_tx,
                            RxEvent::Error {
                                request_id: None,
                                status: ReplyStatus::Rejected,
                                detail: "command rejected during graceful shutdown".to_string(),
                            },
                        );
                    }
                    None => {}
                }
            }
            cmd = command_rx.recv() => {
                match cmd {
                    Some(TxCmd::SendReply { reply }) => {
                        if pending_requests.remove(&reply.request_id).is_some() {
                            try_emit_event(
                                event_tx,
                                RxEvent::IncomingReply {
                                    request_id: reply.request_id,
                                    payload: reply.payload,
                                    headers: reply.headers,
                                    status: reply.status,
                                },
                            );
                        } else {
                            try_emit_event(
                                event_tx,
                                RxEvent::Error {
                                    request_id: Some(reply.request_id),
                                    status: ReplyStatus::Error,
                                    detail: "unexpected reply id".to_string(),
                                },
                            );
                        }
                    }
                    Some(_) => {
                        try_emit_event(
                            event_tx,
                            RxEvent::Error {
                                request_id: None,
                                status: ReplyStatus::Rejected,
                                detail: "command rejected during graceful shutdown".to_string(),
                            },
                        );
                    }
                    None => return Ok(()),
                }
            }
            _ = sleep_until(deadline) => {
                for (request_id, _) in pending_requests.drain() {
                    try_emit_event(
                        event_tx,
                        RxEvent::Error {
                            request_id: Some(request_id),
                            status: ReplyStatus::Timeout,
                            detail: "request timed out during shutdown".to_string(),
                        },
                    );
                }
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "network-tests")]
    use std::sync::atomic::{AtomicU16, Ordering as AtomicOrdering};
    use tokio::sync::Mutex as TokioMutex;
    use tokio::time::timeout;

    #[cfg(feature = "network-tests")]
    static NEXT_TEST_PORT: AtomicU16 = AtomicU16::new(34000);

    struct TestActorChannels {
        command_tx: mpsc::Sender<TxCmd>,
        control_tx: mpsc::Sender<TxCmd>,
        event_rx: mpsc::Receiver<RxEvent>,
    }

    async fn spawn_core(channels: ActorChannels) -> TestActorChannels {
        let ActorChannels {
            command_tx,
            command_rx,
            control_tx,
            control_rx,
            event_tx,
            event_rx,
        } = channels;
        let actor_tx = event_tx.clone();
        tokio::spawn(TransportActor::run_with_channels(command_rx, control_rx, actor_tx));
        TestActorChannels {
            command_tx,
            control_tx,
            event_rx,
        }
    }

    #[cfg(feature = "network-tests")]
    async fn spawn_with_config(config: TransportConfig) -> (TestActorChannels, Arc<TokioMutex<TransportState>>) {
        let channels = bounded_channels(16, 32);
        let ActorChannels {
            command_tx,
            command_rx,
            control_tx,
            control_rx,
            event_tx,
            event_rx,
        } = channels;
        let actor_tx = event_tx.clone();
        let state = Arc::new(TokioMutex::new(TransportState::Running));
        tokio::spawn(TransportActor::run_with_channels_with_config(
            command_rx,
            control_rx,
            actor_tx,
            Arc::clone(&state),
            config,
        ));
        (
            TestActorChannels {
                command_tx,
                control_tx,
                event_rx,
            },
            state,
        )
    }

    #[cfg(feature = "network-tests")]
    fn next_tcp_endpoint() -> String {
        let port = NEXT_TEST_PORT.fetch_add(1, AtomicOrdering::Relaxed);
        format!("tcp://127.0.0.1:{port}")
    }

    #[cfg(feature = "network-tests")]
    async fn recv_until<F>(
        event_rx: &mut mpsc::Receiver<RxEvent>,
        wait_for: Duration,
        mut predicate: F,
    ) -> Option<RxEvent>
    where
        F: FnMut(&RxEvent) -> bool,
    {
        let deadline = Instant::now() + wait_for;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let remaining = deadline - now;
            match timeout(remaining, event_rx.recv()).await {
                Ok(Some(event)) => {
                    if predicate(&event) {
                        return Some(event);
                    }
                }
                Ok(None) | Err(_) => return None,
            }
        }
    }

    #[test]
    fn io_frame_publish_roundtrip_decodes_to_internal_event() {
        let frame = encode_publish_frame("telemetry/temp", b"42").expect("publish frame");
        let event = decode_incoming_message(frame.into()).expect("decode publish");
        match event {
            InternalIoEvent::IncomingPublish { topic, payload } => {
                assert_eq!(topic, "telemetry/temp");
                assert_eq!(payload, b"42".to_vec());
            }
            _ => panic!("expected publish event"),
        }
    }

    #[test]
    fn io_frame_request_roundtrip_decodes_to_internal_event() {
        let request = TxRequest {
            request_id: 99,
            topic: "svc/ping".to_string(),
            payload: b"hello".to_vec(),
            headers: None,
            timeout_ms: Some(123),
        };
        let frame = encode_request_frame(&request).expect("request frame");
        let event = decode_incoming_message(frame.into()).expect("decode request");
        match event {
            InternalIoEvent::IncomingRequest {
                request_id,
                topic,
                payload,
            } => {
                assert_eq!(request_id, 99);
                assert_eq!(topic, "svc/ping");
                assert_eq!(payload, b"hello".to_vec());
            }
            _ => panic!("expected request event"),
        }
    }

    #[test]
    fn io_frame_reply_roundtrip_decodes_to_internal_event() {
        let reply = TxReply {
            request_id: 101,
            payload: b"world".to_vec(),
            status: ReplyStatus::NotFound,
            headers: None,
        };
        let frame = encode_reply_frame(&reply).expect("reply frame");
        let event = decode_incoming_message(frame.into()).expect("decode reply");
        match event {
            InternalIoEvent::IncomingReply {
                request_id,
                status,
                payload,
            } => {
                assert_eq!(request_id, 101);
                assert_eq!(status, ReplyStatus::NotFound);
                assert_eq!(payload, b"world".to_vec());
            }
            _ => panic!("expected reply event"),
        }
    }

    #[test]
    fn io_frame_decode_rejects_unknown_frame_kind() {
        let result = decode_incoming_message(vec![0xff, 0, 1, 2].into());
        assert!(matches!(result, Err(detail) if detail == "unknown zmq message frame"));
    }

    #[tokio::test]
    async fn emit_state_error_updates_state_and_emits_classified_error() {
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let state = Arc::new(TokioMutex::new(TransportState::Running));

        emit_state_error(&state, &event_tx, true, Some(7), "temporary").await;
        assert_eq!(*state.lock().await, TransportState::Degraded);
        match event_rx.recv().await {
            Some(RxEvent::Error {
                request_id: Some(7),
                status: ReplyStatus::Error,
                detail,
            }) => assert!(detail.contains("recoverable I/O error: temporary")),
            _ => panic!("expected recoverable io error"),
        }

        emit_state_error(&state, &event_tx, false, None, "hard-fail").await;
        assert_eq!(*state.lock().await, TransportState::Failed);
        match event_rx.recv().await {
            Some(RxEvent::Error {
                request_id: None,
                status: ReplyStatus::Error,
                detail,
            }) => assert!(detail.contains("fatal I/O error: hard-fail")),
            _ => panic!("expected fatal io error"),
        }
    }

    #[tokio::test]
    async fn recoverable_error_then_recovery_returns_to_running_state() {
        let (event_tx, _event_rx) = mpsc::channel(8);
        let state = Arc::new(TokioMutex::new(TransportState::Running));

        emit_state_error(&state, &event_tx, true, None, "temporary").await;
        assert_eq!(*state.lock().await, TransportState::Degraded);

        emit_transport_recovery(&state).await;
        assert_eq!(*state.lock().await, TransportState::Running);
    }

    #[tokio::test]
    async fn event_queue_overflow_drops_newest_event() {
        let (event_tx, mut event_rx) = mpsc::channel(1);
        event_tx
            .try_send(RxEvent::Connected {
                endpoint: "inproc://first".to_string(),
            })
            .expect("fill event queue");

        try_emit_event(
            &event_tx,
            RxEvent::Connected {
                endpoint: "inproc://dropped".to_string(),
            },
        );

        assert!(matches!(
            event_rx.recv().await,
            Some(RxEvent::Connected { endpoint }) if endpoint == "inproc://first"
        ));
        assert!(matches!(
            timeout(Duration::from_millis(20), event_rx.recv()).await,
            Err(_)
        ));
    }

    #[tokio::test]
    async fn io_event_queue_overflow_drops_newest_event() {
        let (io_tx, mut io_rx) = mpsc::channel(1);
        io_tx
            .try_send(InternalIoEvent::IncomingPublish {
                topic: "telemetry/first".to_string(),
                payload: vec![1],
            })
            .expect("fill io event queue");

        try_emit_io_event(
            &io_tx,
            InternalIoEvent::IncomingPublish {
                topic: "telemetry/dropped".to_string(),
                payload: vec![2],
            },
        );

        assert!(matches!(
            io_rx.recv().await,
            Some(InternalIoEvent::IncomingPublish { topic, payload })
                if topic == "telemetry/first" && payload == vec![1]
        ));
        assert!(matches!(
            timeout(Duration::from_millis(20), io_rx.recv()).await,
            Err(_)
        ));
    }

    #[tokio::test]
    async fn sub_command_queue_reports_full_when_saturated() {
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        sub_tx
            .try_send(SubControlCommand::Subscribe("topic/first".to_string()))
            .expect("fill sub command queue");

        let err = sub_tx
            .try_send(SubControlCommand::Unsubscribe("topic/second".to_string()))
            .expect_err("second command should hit full queue");
        assert!(matches!(
            err,
            mpsc::error::TrySendError::Full(SubControlCommand::Unsubscribe(topic))
                if topic == "topic/second"
        ));
    }

    #[tokio::test]
    async fn zeromq_init_failure_emits_fatal_event_and_transitions_state() {
        let channels = bounded_channels(8, 8);
        let ActorChannels {
            command_tx: _,
            command_rx,
            control_tx,
            control_rx,
            event_tx,
            mut event_rx,
        } = channels;
        let actor_tx = event_tx.clone();
        let state = Arc::new(TokioMutex::new(TransportState::Running));
        let config = TransportConfig {
            enable_zeromq_io: true,
            zeromq_pub_bind: Some("invalid://endpoint".to_string()),
            ..TransportConfig::default()
        };

        tokio::spawn(TransportActor::run_with_channels_with_config(
            command_rx,
            control_rx,
            actor_tx,
            Arc::clone(&state),
            config,
        ));

        match event_rx.recv().await {
            Some(RxEvent::Error {
                request_id: None,
                status: ReplyStatus::Error,
                detail,
            }) => {
                assert!(detail.contains("fatal I/O error"));
                assert!(detail.contains("zeromq initialization failed"));
            }
            _ => panic!("expected fatal io init error event"),
        }
        assert_eq!(*state.lock().await, TransportState::Failed);

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let _ = control_tx
            .send(TxCmd::Shutdown {
                graceful: true,
                timeout_ms: Some(50),
                ack: ack_tx,
            })
            .await;
        assert!(matches!(ack_rx.await, Ok(Ok(()))));
    }

    #[tokio::test]
    async fn actor_emits_correlated_request_and_reply() {
        let mut actor_channels = spawn_core(bounded_channels(16, 16)).await;

        let request_id = 42u64;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::SendRequest {
                request: TxRequest {
                    request_id,
                    topic: "topic/a".to_string(),
                    payload: b"payload".to_vec(),
                    headers: None,
                    timeout_ms: None,
                },
            })
            .await;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::SendReply {
                reply: TxReply {
                    request_id,
                    payload: b"reply".to_vec(),
                    status: ReplyStatus::Ok,
                    headers: None,
                },
            })
            .await;

        let req_event = actor_channels.event_rx.recv().await;
        let rep_event = actor_channels.event_rx.recv().await;
        match req_event {
            Some(RxEvent::IncomingRequest {
                request_id: actual, ..
            }) => assert_eq!(actual, request_id),
            _ => panic!("expected incoming request event"),
        }
        match rep_event {
            Some(RxEvent::IncomingReply {
                request_id: actual,
                status,
                ..
            }) => {
                assert_eq!(actual, request_id);
                assert_eq!(status, ReplyStatus::Ok);
            }
            _ => panic!("expected incoming reply event"),
        }
    }

    #[tokio::test]
    async fn actor_emits_control_and_payload_events() {
        let mut actor_channels = spawn_core(bounded_channels(16, 16)).await;

        let _ = actor_channels
            .command_tx
            .send(TxCmd::Publish {
                topic: "telemetry/temp".to_string(),
                payload: b"42".to_vec(),
                headers: None,
            })
            .await;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::Subscribe {
                topic: "telemetry/+".to_string(),
            })
            .await;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::Unsubscribe {
                topic: "telemetry/+".to_string(),
            })
            .await;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::Connect {
                endpoint: "inproc://bus".to_string(),
                namespace: Some("default".to_string()),
            })
            .await;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::Disconnect {
                endpoint: "inproc://bus".to_string(),
                reason: Some("test".to_string()),
            })
            .await;

        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::IncomingPublish { topic, .. }) if topic == "telemetry/temp"
        ));
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::Subscribed { topic }) if topic == "telemetry/+"
        ));
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::Unsubscribed { topic }) if topic == "telemetry/+"
        ));
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::Connected { endpoint }) if endpoint == "inproc://bus"
        ));
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::Disconnected { endpoint, .. }) if endpoint == "inproc://bus"
        ));
    }

    #[tokio::test]
    async fn actor_rejects_unmatched_reply_with_error_event() {
        let mut actor_channels = spawn_core(bounded_channels(16, 16)).await;

        let unknown_request_id = 77u64;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::SendReply {
                reply: TxReply {
                    request_id: unknown_request_id,
                    payload: b"reply".to_vec(),
                    status: ReplyStatus::Ok,
                    headers: None,
                },
            })
            .await;

        match actor_channels.event_rx.recv().await {
            Some(RxEvent::Error {
                request_id: Some(actual),
                status: ReplyStatus::Error,
                detail,
            }) => {
                assert_eq!(actual, unknown_request_id);
                assert_eq!(detail, "unexpected reply id");
            }
            _ => panic!("expected error event for unmatched reply id"),
        }
    }

    #[tokio::test]
    async fn actor_emits_timeout_error_when_request_not_replied() {
        let mut actor_channels = spawn_core(bounded_channels(16, 16)).await;

        let _ = actor_channels
            .command_tx
            .send(TxCmd::SendRequest {
                request: TxRequest {
                    request_id: 11,
                    topic: "topic/timeout".to_string(),
                    payload: b"req".to_vec(),
                    headers: None,
                    timeout_ms: Some(20),
                },
            })
            .await;

        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::IncomingRequest { request_id: 11, .. })
        ));

        match actor_channels.event_rx.recv().await {
            Some(RxEvent::Error {
                request_id: Some(actual),
                status: ReplyStatus::Timeout,
                detail,
            }) => {
                assert_eq!(actual, 11);
                assert_eq!(detail, "request timed out");
            }
            _ => panic!("expected request timeout error"),
        }
    }

    #[tokio::test]
    async fn actor_shutdown_graceful_waits_for_reply_within_timeout() {
        let mut actor_channels = spawn_core(bounded_channels(16, 16)).await;

        let request_id = 21u64;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::SendRequest {
                request: TxRequest {
                    request_id,
                    topic: "topic/graceful".to_string(),
                    payload: b"req".to_vec(),
                    headers: None,
                    timeout_ms: Some(100),
                },
            })
            .await;
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::IncomingRequest { request_id: 21, .. })
        ));
        let _ = actor_channels
            .control_tx
            .send(TxCmd::Shutdown {
                graceful: true,
                timeout_ms: Some(200),
                ack: {
                    let (ack_tx, _) = tokio::sync::oneshot::channel();
                    ack_tx
                },
            })
            .await;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::SendReply {
                reply: TxReply {
                    request_id,
                    payload: b"reply".to_vec(),
                    status: ReplyStatus::Ok,
                    headers: None,
                },
            })
            .await;

        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::ShutdownRequested)
        ));
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::IncomingReply {
                request_id: 21,
                status: ReplyStatus::Ok,
                ..
            })
        ));
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::ShutdownCompleted)
        ));
    }

    #[tokio::test]
    async fn actor_shutdown_non_graceful_cancels_pending_immediately() {
        let mut actor_channels = spawn_core(bounded_channels(16, 16)).await;

        let request_id = 31u64;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::SendRequest {
                request: TxRequest {
                    request_id,
                    topic: "topic/fast-stop".to_string(),
                    payload: b"req".to_vec(),
                    headers: None,
                    timeout_ms: None,
                },
            })
            .await;
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::IncomingRequest { request_id: 31, .. })
        ));
        let _ = actor_channels
            .control_tx
            .send(TxCmd::Shutdown {
                graceful: false,
                timeout_ms: None,
                ack: {
                    let (ack_tx, _) = tokio::sync::oneshot::channel();
                    ack_tx
                },
            })
            .await;

        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::ShutdownRequested)
        ));
        match actor_channels.event_rx.recv().await {
            Some(RxEvent::Error {
                request_id: Some(actual),
                status: ReplyStatus::Error,
                ..
            }) => assert_eq!(actual, request_id),
            _ => panic!("expected error event for shutdown cancellation"),
        }
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::ShutdownCompleted)
        ));
    }

    #[tokio::test]
    async fn actor_shutdown_graceful_rejects_new_commands_until_complete() {
        let mut actor_channels = spawn_core(bounded_channels(16, 16)).await;

        let request_id = 41u64;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::SendRequest {
                request: TxRequest {
                    request_id,
                    topic: "topic/reject".to_string(),
                    payload: b"req".to_vec(),
                    headers: None,
                    timeout_ms: None,
                },
            })
            .await;
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::IncomingRequest { request_id: 41, .. })
        ));
        let _ = actor_channels
            .control_tx
            .send(TxCmd::Shutdown {
                graceful: true,
                timeout_ms: Some(200),
                ack: {
                    let (ack_tx, _) = tokio::sync::oneshot::channel();
                    ack_tx
                },
            })
            .await;
        let _ = actor_channels
            .command_tx
            .send(TxCmd::Publish {
                topic: "topic/reject".to_string(),
                payload: b"late".to_vec(),
                headers: None,
            })
            .await;

        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::ShutdownRequested)
        ));
        match actor_channels.event_rx.recv().await {
            Some(RxEvent::Error {
                request_id: None,
                status: ReplyStatus::Rejected,
                ..
            }) => {}
            _ => panic!("expected rejected command during graceful shutdown"),
        }
    }

    #[tokio::test]
    async fn actor_emits_shutdown_events_in_order() {
        let mut actor_channels = spawn_core(bounded_channels(16, 16)).await;

        let _ = actor_channels
            .control_tx
            .send(TxCmd::Shutdown {
                graceful: true,
                timeout_ms: None,
                ack: {
                    let (ack_tx, _) = tokio::sync::oneshot::channel();
                    ack_tx
                },
            })
            .await;

        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::ShutdownRequested)
        ));
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::ShutdownCompleted)
        ));
    }

    #[tokio::test]
    async fn actor_prioritizes_control_shutdown_over_normal_commands() {
        let mut actor_channels = spawn_core(bounded_channels(16, 16)).await;

        let _ = actor_channels
            .command_tx
            .send(TxCmd::Publish {
                topic: "topic/priority".to_string(),
                payload: b"late".to_vec(),
                headers: None,
            })
            .await;
        let _ = actor_channels
            .control_tx
            .send(TxCmd::Shutdown {
                graceful: false,
                timeout_ms: None,
                ack: {
                    let (ack_tx, _) = tokio::sync::oneshot::channel();
                    ack_tx
                },
            })
            .await;

        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::ShutdownRequested)
        ));
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::ShutdownCompleted)
        ));
        assert!(matches!(actor_channels.event_rx.recv().await, None));
    }

    #[tokio::test]
    async fn actor_shutdown_returns_acknowledgement() {
        let mut actor_channels = spawn_core(bounded_channels(16, 16)).await;
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();

        let _ = actor_channels
            .control_tx
            .send(TxCmd::Shutdown {
                graceful: true,
                timeout_ms: None,
                ack: ack_tx,
            })
            .await;

        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::ShutdownRequested)
        ));
        assert!(matches!(
            ack_rx.await,
            Ok(Ok(()))
        ));
        assert!(matches!(
            actor_channels.event_rx.recv().await,
            Some(RxEvent::ShutdownCompleted)
        ));
    }

    #[cfg(feature = "network-tests")]
    #[tokio::test]
    #[ignore = "requires zeromq bind/connect permissions"]
    async fn zeromq_pub_sub_path_delivers_remote_message() {
        let endpoint = next_tcp_endpoint();
        let pub_config = TransportConfig {
            enable_zeromq_io: true,
            zeromq_pub_bind: Some(endpoint.clone()),
            ..TransportConfig::default()
        };
        let sub_config = TransportConfig {
            enable_zeromq_io: true,
            zeromq_sub_connect: vec![endpoint],
            ..TransportConfig::default()
        };

        let (publisher, publisher_state) = spawn_with_config(pub_config).await;
        sleep(Duration::from_millis(200)).await;
        let (mut subscriber, _) = spawn_with_config(sub_config).await;
        sleep(Duration::from_millis(200)).await;
        if *publisher_state.lock().await == TransportState::Failed {
            eprintln!("skipping: zeromq bind/connect not permitted in this environment");
            return;
        }

        let _ = subscriber
            .command_tx
            .send(TxCmd::Subscribe {
                topic: "telemetry/temp".to_string(),
            })
            .await;
        assert!(matches!(
            recv_until(&mut subscriber.event_rx, Duration::from_secs(1), |event| {
                matches!(event, RxEvent::Subscribed { topic } if topic == "telemetry/temp")
            })
            .await,
            Some(RxEvent::Subscribed { .. })
        ));

        sleep(Duration::from_millis(100)).await;

        let mut delivered = false;
        let mut observed = Vec::new();
        for _ in 0..20 {
            let _ = publisher
                .command_tx
                .send(TxCmd::Publish {
                    topic: "telemetry/temp".to_string(),
                    payload: b"42".to_vec(),
                    headers: None,
                })
                .await;

            if let Ok(event_opt) =
                timeout(Duration::from_millis(150), subscriber.event_rx.recv()).await
            {
                if let Some(event) = event_opt {
                    match &event {
                        RxEvent::IncomingPublish { topic, payload, .. }
                            if topic == "telemetry/temp" && payload == b"42" =>
                        {
                            delivered = true;
                            break;
                        }
                        _ => observed.push(format!("{event:?}")),
                    }
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(
            delivered,
            "publish message was not delivered within retry window; observed events: {observed:?}"
        );

        let (ack_tx1, ack_rx1) = tokio::sync::oneshot::channel();
        let _ = publisher
            .control_tx
            .send(TxCmd::Shutdown {
                graceful: false,
                timeout_ms: None,
                ack: ack_tx1,
            })
            .await;
        let (ack_tx2, ack_rx2) = tokio::sync::oneshot::channel();
        let _ = subscriber
            .control_tx
            .send(TxCmd::Shutdown {
                graceful: false,
                timeout_ms: None,
                ack: ack_tx2,
            })
            .await;
        assert!(matches!(
            timeout(Duration::from_secs(2), ack_rx1).await,
            Ok(Ok(Ok(())))
        ));
        assert!(matches!(
            timeout(Duration::from_secs(2), ack_rx2).await,
            Ok(Ok(Ok(())))
        ));
    }

    #[cfg(feature = "network-tests")]
    #[tokio::test]
    #[ignore = "requires zeromq bind/connect permissions"]
    async fn zeromq_request_reply_roundtrip_works_across_actors() {
        let endpoint = next_tcp_endpoint();
        let server_config = TransportConfig {
            enable_zeromq_io: true,
            zeromq_req_bind: Some(endpoint.clone()),
            ..TransportConfig::default()
        };
        let client_config = TransportConfig {
            enable_zeromq_io: true,
            zeromq_req_connect: vec![endpoint],
            ..TransportConfig::default()
        };

        let (mut server, server_state) = spawn_with_config(server_config).await;
        sleep(Duration::from_millis(200)).await;
        let (mut client, client_state) = spawn_with_config(client_config).await;
        sleep(Duration::from_millis(200)).await;
        if *server_state.lock().await == TransportState::Failed
            || *client_state.lock().await == TransportState::Failed
        {
            eprintln!("skipping: zeromq bind/connect not permitted in this environment");
            return;
        }

        let request_id = 9001u64;
        let _ = client
            .command_tx
            .send(TxCmd::SendRequest {
                request: TxRequest {
                    request_id,
                    topic: "svc/echo".to_string(),
                    payload: b"ping".to_vec(),
                    headers: None,
                    timeout_ms: Some(1000),
                },
            })
            .await;

        assert!(matches!(
            recv_until(&mut server.event_rx, Duration::from_secs(2), |event| {
                matches!(
                    event,
                    RxEvent::IncomingRequest { request_id: rid, topic, payload, .. }
                        if *rid == request_id && topic == "svc/echo" && payload == b"ping"
                )
            })
            .await,
            Some(RxEvent::IncomingRequest { .. })
        ));

        let _ = server
            .command_tx
            .send(TxCmd::SendReply {
                reply: TxReply {
                    request_id,
                    payload: b"pong".to_vec(),
                    status: ReplyStatus::Ok,
                    headers: None,
                },
            })
            .await;

        assert!(matches!(
            recv_until(&mut client.event_rx, Duration::from_secs(2), |event| {
                matches!(
                    event,
                    RxEvent::IncomingReply { request_id: rid, payload, status, .. }
                        if *rid == request_id && payload == b"pong" && *status == ReplyStatus::Ok
                )
            })
            .await,
            Some(RxEvent::IncomingReply { .. })
        ));

        let (ack_tx1, ack_rx1) = tokio::sync::oneshot::channel();
        let _ = server
            .control_tx
            .send(TxCmd::Shutdown {
                graceful: false,
                timeout_ms: None,
                ack: ack_tx1,
            })
            .await;
        let (ack_tx2, ack_rx2) = tokio::sync::oneshot::channel();
        let _ = client
            .control_tx
            .send(TxCmd::Shutdown {
                graceful: false,
                timeout_ms: None,
                ack: ack_tx2,
            })
            .await;
        assert!(matches!(
            timeout(Duration::from_secs(2), ack_rx1).await,
            Ok(Ok(Ok(())))
        ));
        assert!(matches!(
            timeout(Duration::from_secs(2), ack_rx2).await,
            Ok(Ok(Ok(())))
        ));
    }

    #[cfg(feature = "network-tests")]
    #[tokio::test]
    #[ignore = "requires zeromq bind/connect permissions"]
    async fn zeromq_invalid_frame_surfaces_error_and_degrades_state() {
        let endpoint = next_tcp_endpoint();
        let mut raw_pub = PubSocket::new();
        if raw_pub.bind(&endpoint).await.is_err() {
            eprintln!("skipping: zeromq bind/connect not permitted in this environment");
            return;
        }

        let sub_config = TransportConfig {
            enable_zeromq_io: true,
            zeromq_sub_connect: vec![endpoint],
            ..TransportConfig::default()
        };
        let (mut subscriber, state) = spawn_with_config(sub_config).await;

        let _ = subscriber
            .command_tx
            .send(TxCmd::Subscribe {
                topic: "".to_string(),
            })
            .await;
        let _ = recv_until(&mut subscriber.event_rx, Duration::from_millis(500), |event| {
            matches!(event, RxEvent::Subscribed { .. })
        })
        .await;
        sleep(Duration::from_millis(100)).await;

        let invalid: ZmqMessage = vec![0xff, 0x00, 0x01].into();
        raw_pub.send(invalid).await.expect("send invalid frame");

        assert!(matches!(
            recv_until(&mut subscriber.event_rx, Duration::from_secs(2), |event| {
                matches!(
                    event,
                    RxEvent::Error { status: ReplyStatus::Error, detail, .. }
                        if detail.contains("recoverable I/O error")
                )
            })
            .await,
            Some(RxEvent::Error { .. })
        ));
        assert_eq!(*state.lock().await, TransportState::Degraded);

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let _ = subscriber
            .control_tx
            .send(TxCmd::Shutdown {
                graceful: false,
                timeout_ms: None,
                ack: ack_tx,
            })
            .await;
        assert!(matches!(
            timeout(Duration::from_secs(2), ack_rx).await,
            Ok(Ok(Ok(())))
        ));
    }
}
