//! Internal actor message abstractions for the async runtime.
//!
//! This module defines transport-agnostic command and event types used by the v2 actor loop.
//! ZeroMQ/socket-specific payloads and API details are intentionally absent and should be
//! mapped in the adapter layer instead.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::{sleep, sleep_until, Duration, Instant};
use tokio::sync::oneshot;

use crate::error::TransportResult;

/// Default capacity for the inbound command channel.
pub const DEFAULT_COMMAND_CHANNEL_CAPACITY: usize = 128;
/// Default capacity for the outbound event channel.
pub const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 256;
/// Default graceful shutdown timeout in milliseconds.
pub const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 10_000;

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
    let (command_tx, command_rx) = mpsc::channel(command_capacity);
    let (control_tx, control_rx) = mpsc::channel(command_capacity);
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
    /// Placeholder event loop that receives bounded inbound commands and emits outbound events.
    /// The loop currently acts as a smoke-safe sink for command/event plumbing.
    pub async fn run_with_channels(
        command_rx: mpsc::Receiver<TxCmd>,
        control_rx: mpsc::Receiver<TxCmd>,
        event_tx: mpsc::Sender<RxEvent>,
    ) {
        Self::run_loop(command_rx, control_rx, event_tx).await;
    }

    /// Core actor entrypoint.
    pub async fn run_loop(
        mut command_rx: mpsc::Receiver<TxCmd>,
        mut control_rx: mpsc::Receiver<TxCmd>,
        event_tx: mpsc::Sender<RxEvent>,
    ) {
        let pending_requests: Arc<Mutex<HashMap<RequestId, Option<u64>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut command_open = true;
        let mut control_open = true;

        while command_open || control_open {
            tokio::select! {
                biased;
                cmd = control_rx.recv(), if control_open => {
                    match cmd {
                        Some(TxCmd::Shutdown {
                            graceful,
                            timeout_ms,
                            ack,
                        }) => {
                            let _ = event_tx.send(RxEvent::ShutdownRequested).await;
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
                            let _ = event_tx.send(RxEvent::ShutdownCompleted).await;
                            let _ = ack.send(result);
                            return;
                        }
                        Some(_) => {
                            let _ = event_tx
                                .send(RxEvent::Error {
                                    request_id: None,
                                    status: ReplyStatus::Rejected,
                                    detail: "non-shutdown command sent on control channel".to_string(),
                                })
                                .await;
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
                            let _ = event_tx
                                .send(RxEvent::IncomingPublish {
                                    topic,
                                    payload,
                                    headers,
                                })
                                .await;
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
                                        let _ = timeout_tx
                                            .send(RxEvent::Error {
                                                request_id: Some(request_id),
                                                status: ReplyStatus::Timeout,
                                                detail: "request timed out".to_string(),
                                            })
                                            .await;
                                    }
                                });
                            }

                            let _ = event_tx
                                .send(RxEvent::IncomingRequest {
                                    request_id,
                                    topic: request.topic,
                                    payload: request.payload,
                                    headers: request.headers,
                                })
                                .await;
                        }
                        Some(TxCmd::SendReply { reply }) => {
                            let mut pending = pending_requests.lock().await;
                            if pending.remove(&reply.request_id).is_some() {
                                let _ = event_tx
                                    .send(RxEvent::IncomingReply {
                                        request_id: reply.request_id,
                                        payload: reply.payload,
                                        headers: reply.headers,
                                        status: reply.status,
                                    })
                                    .await;
                            } else {
                                let _ = event_tx
                                    .send(RxEvent::Error {
                                        request_id: Some(reply.request_id),
                                        status: ReplyStatus::Error,
                                        detail: "unexpected reply id".to_string(),
                                    })
                                    .await;
                            }
                        }
                        Some(TxCmd::Subscribe { topic }) => {
                            let _ = event_tx.send(RxEvent::Subscribed { topic }).await;
                        }
                        Some(TxCmd::Unsubscribe { topic }) => {
                            let _ = event_tx.send(RxEvent::Unsubscribed { topic }).await;
                        }
                        Some(TxCmd::Connect { endpoint, .. }) => {
                            let _ = event_tx.send(RxEvent::Connected { endpoint }).await;
                        }
                        Some(TxCmd::Disconnect { endpoint, reason }) => {
                            let _ = event_tx
                                .send(RxEvent::Disconnected {
                                    endpoint,
                                    reason,
                                })
                                .await;
                        }
                        Some(TxCmd::Shutdown { .. }) => {
                            let _ = event_tx
                                .send(RxEvent::Error {
                                    request_id: None,
                                    status: ReplyStatus::Rejected,
                                    detail: "shutdown command sent on normal channel".to_string(),
                                })
                                .await;
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
            let _ = event_tx
                .send(RxEvent::Error {
                    request_id: Some(request_id),
                    status: ReplyStatus::Error,
                    detail: "actor shutting down".to_string(),
                })
                .await;
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
                        let _ = event_tx
                            .send(RxEvent::Error {
                                request_id: None,
                                status: ReplyStatus::Rejected,
                                detail: "command rejected during graceful shutdown".to_string(),
                            })
                            .await;
                    }
                    None => {}
                }
            }
            cmd = command_rx.recv() => {
                match cmd {
                    Some(TxCmd::SendReply { reply }) => {
                        if pending_requests.remove(&reply.request_id).is_some() {
                            let _ = event_tx
                                .send(RxEvent::IncomingReply {
                                    request_id: reply.request_id,
                                    payload: reply.payload,
                                    headers: reply.headers,
                                    status: reply.status,
                                })
                                .await;
                        } else {
                            let _ = event_tx
                                .send(RxEvent::Error {
                                    request_id: Some(reply.request_id),
                                    status: ReplyStatus::Error,
                                    detail: "unexpected reply id".to_string(),
                                })
                                .await;
                        }
                    }
                    Some(_) => {
                        let _ = event_tx
                            .send(RxEvent::Error {
                                request_id: None,
                                status: ReplyStatus::Rejected,
                                detail: "command rejected during graceful shutdown".to_string(),
                            })
                            .await;
                    }
                    None => return Ok(()),
                }
            }
            _ = sleep_until(deadline) => {
                for (request_id, _) in pending_requests.drain() {
                    let _ = event_tx
                        .send(RxEvent::Error {
                            request_id: Some(request_id),
                            status: ReplyStatus::Timeout,
                            detail: "request timed out during shutdown".to_string(),
                        })
                        .await;
                }
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
