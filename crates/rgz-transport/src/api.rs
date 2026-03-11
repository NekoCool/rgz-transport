use crate::actor::TransportActor;
use crate::actor::{
    DEFAULT_SHUTDOWN_TIMEOUT_MS, ReplyStatus, RequestId, RequestIdGenerator, RxEvent, TxCmd,
    TxReply, TxRequest, bounded_channels_with_control,
};
use crate::config::TransportConfig;
use crate::error::{TransportError, TransportResult};
use crate::metrics::{TransportMetrics, TransportMetricsSnapshot};
use crate::state::{TransportEvent, TransportState, transition};
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, timeout};

/// Main transport entrypoint for v2.
pub struct Transport {
    state: Arc<Mutex<TransportState>>,
    config: TransportConfig,
}

/// Handle returned to users to interact with a running transport instance.
pub struct TransportHandle {
    state: Arc<Mutex<TransportState>>,
    config: TransportConfig,
    metrics: Arc<TransportMetrics>,
    command_tx: mpsc::Sender<TxCmd>,
    control_tx: mpsc::Sender<TxCmd>,
    event_rx: Arc<Mutex<mpsc::Receiver<RxEvent>>>,
    request_id_gen: RequestIdGenerator,
    actor_task: tokio::task::JoinHandle<()>,
}

impl Transport {
    pub fn new(config: TransportConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(TransportState::Created)),
            config,
        }
    }

    pub async fn start(self) -> TransportResult<TransportHandle> {
        let next = {
            let state = self.state.lock().await;
            transition(*state, TransportEvent::InitRequested).map_err(|_| {
                TransportError::InvalidTransition {
                    from: *state,
                    event: TransportEvent::InitRequested,
                }
            })?
        };

        {
            let mut state = self.state.lock().await;
            *state = next;
        }

        let started = {
            let state = self.state.lock().await;
            transition(*state, TransportEvent::StartOk).map_err(|_| {
                TransportError::InvalidTransition {
                    from: *state,
                    event: TransportEvent::StartOk,
                }
            })?
        };

        {
            let mut state = self.state.lock().await;
            *state = started;
        }

        let actor_channels = bounded_channels_with_control(
            self.config.command_channel_capacity,
            self.config.control_channel_capacity,
            self.config.event_channel_capacity,
        );
        let metrics = Arc::new(TransportMetrics::default());
        let actor_tx = actor_channels.event_tx.clone();
        let actor_task = tokio::spawn(TransportActor::run_with_channels_with_config(
            actor_channels.command_rx,
            actor_channels.control_rx,
            actor_tx,
            Arc::clone(&self.state),
            self.config.clone(),
            Arc::clone(&metrics),
        ));

        Ok(TransportHandle {
            state: Arc::clone(&self.state),
            config: self.config,
            metrics,
            command_tx: actor_channels.command_tx,
            control_tx: actor_channels.control_tx,
            event_rx: Arc::new(Mutex::new(actor_channels.event_rx)),
            request_id_gen: RequestIdGenerator::new(),
            actor_task,
        })
    }
}

impl TransportHandle {
    /// Returns immutable transport configuration used at construction time.
    pub fn config(&self) -> &TransportConfig {
        &self.config
    }

    pub fn command_sender(&self) -> mpsc::Sender<TxCmd> {
        self.command_tx.clone()
    }

    pub fn control_sender(&self) -> mpsc::Sender<TxCmd> {
        self.control_tx.clone()
    }

    pub fn next_request_id(&self) -> RequestId {
        self.request_id_gen.next()
    }

    pub fn metrics_snapshot(&self) -> TransportMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn try_send_command(&self, cmd: TxCmd) -> TransportResult<()> {
        match self.command_tx.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.metrics.inc_command_full();
                Err(TransportError::NodeBusy { path: "command" })
            }
            Err(TrySendError::Closed(_)) => Err(TransportError::NotRunning),
        }
    }

    fn try_send_control(&self, cmd: TxCmd) -> TransportResult<()> {
        match self.control_tx.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.metrics.inc_control_full();
                Err(TransportError::NodeBusy { path: "control" })
            }
            Err(TrySendError::Closed(_)) => Err(TransportError::NotRunning),
        }
    }

    pub async fn send_cmd(&self, cmd: TxCmd) -> TransportResult<()> {
        match cmd {
            TxCmd::Shutdown { .. } => self.try_send_control(cmd),
            _ => self.try_send_command(cmd),
        }
    }

    pub async fn publish(
        &self,
        topic: impl Into<String>,
        payload: Vec<u8>,
        headers: Option<crate::actor::MessageHeaders>,
    ) -> TransportResult<()> {
        self.try_send_command(TxCmd::Publish {
            topic: topic.into(),
            payload,
            headers,
        })
    }

    pub async fn request(
        &self,
        topic: impl Into<String>,
        payload: Vec<u8>,
        headers: Option<crate::actor::MessageHeaders>,
        timeout_ms: Option<u64>,
    ) -> TransportResult<TxRequest> {
        let request_id = self.request_id_gen.next();
        let request = TxRequest {
            request_id,
            topic: topic.into(),
            payload,
            headers,
            timeout_ms,
        };
        self.try_send_command(TxCmd::SendRequest {
            request: request.clone(),
        })?;
        Ok(request)
    }

    pub async fn reply(
        &self,
        request_id: RequestId,
        payload: Vec<u8>,
        status: ReplyStatus,
    ) -> TransportResult<()> {
        self.try_send_command(TxCmd::SendReply {
            reply: TxReply {
                request_id,
                payload,
                status,
                headers: None,
            },
        })
    }

    pub async fn subscribe(&self, topic: impl Into<String>) -> TransportResult<()> {
        self.try_send_command(TxCmd::Subscribe {
            topic: topic.into(),
        })
    }

    pub async fn unsubscribe(&self, topic: impl Into<String>) -> TransportResult<()> {
        self.try_send_command(TxCmd::Unsubscribe {
            topic: topic.into(),
        })
    }

    pub async fn connect(
        &self,
        endpoint: impl Into<String>,
        namespace: Option<String>,
    ) -> TransportResult<()> {
        self.try_send_command(TxCmd::Connect {
            endpoint: endpoint.into(),
            namespace,
        })
    }

    pub async fn disconnect(
        &self,
        endpoint: impl Into<String>,
        reason: Option<String>,
    ) -> TransportResult<()> {
        self.try_send_command(TxCmd::Disconnect {
            endpoint: endpoint.into(),
            reason,
        })
    }

    pub async fn shutdown_cmd(
        &self,
        graceful: bool,
        timeout_ms: Option<u64>,
    ) -> TransportResult<()> {
        let (ack_tx, _ack_rx) = oneshot::channel();
        self.try_send_control(TxCmd::Shutdown {
            graceful,
            timeout_ms,
            ack: ack_tx,
        })
    }

    pub async fn next_event(&self) -> Option<RxEvent> {
        let mut event_rx = self.event_rx.lock().await;
        event_rx.recv().await
    }

    /// Returns the latest observed state.
    pub async fn state(&self) -> TransportState {
        *self.state.lock().await
    }

    /// Request graceful or immediate shutdown and wait for completion event.
    ///
    /// `graceful = true` waits up to `timeout_ms` for outstanding requests to be
    /// resolved via `SendReply`; if timeout is omitted, `DEFAULT_SHUTDOWN_TIMEOUT_MS` is used.
    /// After timeout, pending requests are emitted as timeout errors and shutdown proceeds.
    ///
    /// `graceful = false` immediately resolves all outstanding requests as shutdown errors.
    pub async fn shutdown_with(
        &self,
        graceful: bool,
        timeout_ms: Option<u64>,
    ) -> TransportResult<()> {
        let timeout_ms = timeout_ms.unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT_MS);
        let next_state = {
            let state = self.state.lock().await;
            if *state == TransportState::Stopped {
                return Err(TransportError::NotRunning);
            }

            transition(*state, TransportEvent::ShutdownRequested)?
        };

        let (ack_tx, ack_rx) = oneshot::channel();
        self.try_send_control(TxCmd::Shutdown {
            graceful,
            timeout_ms: Some(timeout_ms),
            ack: ack_tx,
        })?;

        {
            let mut state = self.state.lock().await;
            *state = next_state;
        }

        self.wait_for_shutdown_complete(timeout_ms, ack_rx).await?;

        {
            let mut state = self.state.lock().await;
            let next = transition(*state, TransportEvent::ShutdownComplete)?;
            *state = next;
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> TransportResult<()> {
        self.shutdown_with(true, None).await
    }

    async fn wait_for_shutdown_complete(
        &self,
        timeout_ms: u64,
        ack_rx: oneshot::Receiver<TransportResult<()>>,
    ) -> TransportResult<()> {
        let wait_res = timeout(Duration::from_millis(timeout_ms), ack_rx).await;

        match wait_res {
            Ok(result) => result.map_err(|_| TransportError::NotRunning)?,
            Err(_) => {
                self.actor_task.abort();
                let mut state = self.state.lock().await;
                let next = transition(*state, TransportEvent::ShutdownComplete)?;
                *state = next;
                Err(TransportError::Timeout)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn transport_starts_in_running_state() {
        let transport = Transport::new(TransportConfig::default());
        let handle = transport.start().await.expect("start transport");

        let state = handle.state().await;
        assert_eq!(state, TransportState::Running);
    }

    #[tokio::test]
    async fn transport_shutdown_waits_until_shutdown_completed_before_state_stops() {
        let transport = Transport::new(TransportConfig::default());
        let handle = transport.start().await.expect("start transport");

        handle
            .shutdown_with(true, Some(500))
            .await
            .expect("shutdown");

        let state = handle.state().await;
        assert_eq!(state, TransportState::Stopped);

        let second = handle.shutdown().await;
        assert!(matches!(second, Err(TransportError::NotRunning)));
    }

    #[tokio::test]
    async fn transport_concurrent_shutdown_requests_are_single_completion() {
        let transport = Transport::new(TransportConfig::default());
        let handle = transport.start().await.expect("start transport");

        let f1 = handle.shutdown_with(true, Some(200));
        let f2 = handle.shutdown_with(true, Some(200));
        let (first, second) = tokio::join!(f1, f2);

        let ok_count = usize::from(first.is_ok()) + usize::from(second.is_ok());
        assert_eq!(ok_count, 1);
        assert!(matches!(
            first,
            Ok(())
                | Err(TransportError::InvalidTransition { .. })
                | Err(TransportError::NotRunning)
        ));
        assert!(matches!(
            second,
            Ok(())
                | Err(TransportError::InvalidTransition { .. })
                | Err(TransportError::NotRunning)
        ));

        assert_eq!(handle.state().await, TransportState::Stopped);
    }

    #[tokio::test]
    async fn publish_returns_node_busy_when_command_queue_is_full() {
        let (command_tx, _command_rx) = mpsc::channel(1);
        command_tx
            .try_send(TxCmd::Publish {
                topic: "topic/full".to_string(),
                payload: vec![1],
                headers: None,
            })
            .expect("fill command queue");
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (_event_tx, event_rx) = mpsc::channel(1);

        let handle = TransportHandle {
            state: Arc::new(Mutex::new(TransportState::Running)),
            config: TransportConfig::default(),
            metrics: Arc::new(TransportMetrics::default()),
            command_tx,
            control_tx,
            event_rx: Arc::new(Mutex::new(event_rx)),
            request_id_gen: RequestIdGenerator::new(),
            actor_task: tokio::spawn(async {}),
        };

        let result = handle.publish("topic/full", vec![2], None).await;
        assert!(matches!(
            result,
            Err(TransportError::NodeBusy { path: "command" })
        ));
    }

    #[tokio::test]
    async fn shutdown_cmd_returns_node_busy_when_control_queue_is_full() {
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (_event_tx, event_rx) = mpsc::channel(1);
        let (ack_tx, _ack_rx) = oneshot::channel();
        control_tx
            .try_send(TxCmd::Shutdown {
                graceful: true,
                timeout_ms: Some(10),
                ack: ack_tx,
            })
            .expect("fill control queue");

        let handle = TransportHandle {
            state: Arc::new(Mutex::new(TransportState::Running)),
            config: TransportConfig::default(),
            metrics: Arc::new(TransportMetrics::default()),
            command_tx,
            control_tx,
            event_rx: Arc::new(Mutex::new(event_rx)),
            request_id_gen: RequestIdGenerator::new(),
            actor_task: tokio::spawn(async {}),
        };

        let result = handle.shutdown_cmd(true, Some(10)).await;
        assert!(matches!(
            result,
            Err(TransportError::NodeBusy { path: "control" })
        ));
    }
}
