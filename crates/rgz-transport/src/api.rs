use crate::actor::{
    bounded_channels, ReplyStatus, TxCmd, TxRequest, TxReply, DEFAULT_COMMAND_CHANNEL_CAPACITY,
    DEFAULT_EVENT_CHANNEL_CAPACITY, DEFAULT_SHUTDOWN_TIMEOUT_MS, RxEvent, RequestId,
    RequestIdGenerator,
};
use crate::actor::TransportActor;
use crate::config::TransportConfig;
use crate::error::{TransportError, TransportResult};
use crate::state::{TransportEvent, TransportState, transition};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{timeout, Duration};

/// Main transport entrypoint for v2.
pub struct Transport {
    state: Arc<Mutex<TransportState>>,
    config: TransportConfig,
}

/// Handle returned to users to interact with a running transport instance.
pub struct TransportHandle {
    state: Arc<Mutex<TransportState>>,
    config: TransportConfig,
    command_tx: mpsc::Sender<TxCmd>,
    event_rx: Arc<Mutex<mpsc::Receiver<RxEvent>>>,
    request_id_gen: RequestIdGenerator,
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
            transition(*state, TransportEvent::StartOk).map_err(|_| TransportError::InvalidTransition {
                from: *state,
                event: TransportEvent::StartOk,
            })?
        };

        {
            let mut state = self.state.lock().await;
            *state = started;
        }

        let actor_channels = bounded_channels(
            DEFAULT_COMMAND_CHANNEL_CAPACITY,
            DEFAULT_EVENT_CHANNEL_CAPACITY,
        );
        let actor_tx = actor_channels.event_tx.clone();
        tokio::spawn(TransportActor::run_with_channels(
            actor_channels.command_rx,
            actor_tx,
        ));

        Ok(TransportHandle {
            state: Arc::clone(&self.state),
            config: self.config,
            command_tx: actor_channels.command_tx,
            event_rx: Arc::new(Mutex::new(actor_channels.event_rx)),
            request_id_gen: RequestIdGenerator::new(),
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

    pub fn next_request_id(&self) -> RequestId {
        self.request_id_gen.next()
    }

    pub async fn send_cmd(&self, cmd: TxCmd) {
        let _ = self.command_tx.send(cmd).await;
    }

    pub async fn publish(
        &self,
        topic: impl Into<String>,
        payload: Vec<u8>,
        headers: Option<crate::actor::MessageHeaders>,
    ) {
        let _ = self
            .command_tx
            .send(TxCmd::Publish {
                topic: topic.into(),
                payload,
                headers,
            })
            .await;
    }

    pub async fn request(
        &self,
        topic: impl Into<String>,
        payload: Vec<u8>,
        headers: Option<crate::actor::MessageHeaders>,
        timeout_ms: Option<u64>,
    ) -> TxRequest {
        let request_id = self.request_id_gen.next();
        let request = TxRequest {
            request_id,
            topic: topic.into(),
            payload,
            headers,
            timeout_ms,
        };
        let _ = self
            .command_tx
            .send(TxCmd::SendRequest {
                request: request.clone(),
            })
            .await;
        request
    }

    pub async fn reply(&self, request_id: RequestId, payload: Vec<u8>, status: ReplyStatus) {
        let _ = self
            .command_tx
            .send(TxCmd::SendReply {
                reply: TxReply {
                    request_id,
                    payload,
                    status,
                    headers: None,
                },
            })
            .await;
    }

    pub async fn subscribe(&self, topic: impl Into<String>) {
        let _ = self
            .command_tx
            .send(TxCmd::Subscribe { topic: topic.into() })
            .await;
    }

    pub async fn unsubscribe(&self, topic: impl Into<String>) {
        let _ = self
            .command_tx
            .send(TxCmd::Unsubscribe { topic: topic.into() })
            .await;
    }

    pub async fn connect(&self, endpoint: impl Into<String>, namespace: Option<String>) {
        let _ = self
            .command_tx
            .send(TxCmd::Connect {
                endpoint: endpoint.into(),
                namespace,
            })
            .await;
    }

    pub async fn disconnect(&self, endpoint: impl Into<String>, reason: Option<String>) {
        let _ = self
            .command_tx
            .send(TxCmd::Disconnect {
                endpoint: endpoint.into(),
                reason,
            })
            .await;
    }

    pub async fn shutdown_cmd(&self, graceful: bool, timeout_ms: Option<u64>) {
        let _ = self
            .command_tx
            .send(TxCmd::Shutdown {
                graceful,
                timeout_ms,
            })
            .await;
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
        let next_state = {
            let state = self.state.lock().await;
            if *state == TransportState::Stopped {
                return Err(TransportError::NotRunning);
            }

            transition(*state, TransportEvent::ShutdownRequested)?
        };

        self
            .command_tx
            .send(TxCmd::Shutdown {
                graceful,
                timeout_ms,
            })
            .await
            .map_err(|_| TransportError::NotRunning)?;

        {
            let mut state = self.state.lock().await;
            *state = next_state;
        }

        self.wait_for_shutdown_complete(timeout_ms.unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT_MS))
            .await?;

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

    async fn wait_for_shutdown_complete(&self, timeout_ms: u64) -> TransportResult<()> {
        let mut event_rx = self.event_rx.lock().await;
        let wait_res = timeout(
            Duration::from_millis(timeout_ms),
            async {
                while let Some(event) = event_rx.recv().await {
                    match event {
                        RxEvent::ShutdownCompleted => return Ok(()),
                        _ => continue,
                    }
                }

                Err(TransportError::NotRunning)
            },
        )
        .await;

        match wait_res {
            Ok(result) => result,
            Err(_) => Err(TransportError::Timeout),
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
}
