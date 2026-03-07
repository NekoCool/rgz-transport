use crate::config::TransportConfig;
use crate::error::{TransportError, TransportResult};
use crate::state::{TransportEvent, TransportState, transition};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Main transport entrypoint for v2.
pub struct Transport {
    state: Arc<Mutex<TransportState>>,
    config: TransportConfig,
}

/// Handle returned to users to interact with a running transport instance.
pub struct TransportHandle {
    state: Arc<Mutex<TransportState>>,
    config: TransportConfig,
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

        Ok(TransportHandle {
            state: Arc::clone(&self.state),
            config: self.config,
        })
    }
}

impl TransportHandle {
    pub fn config(&self) -> &TransportConfig {
        &self.config
    }

    pub async fn state(&self) -> TransportState {
        *self.state.lock().await
    }

    pub async fn shutdown(&self) -> TransportResult<()> {
        {
            let mut state = self.state.lock().await;
            if *state == TransportState::Stopped {
                return Err(TransportError::NotRunning);
            }
            let next = transition(*state, TransportEvent::ShutdownRequested)?;
            *state = next;
        }

        {
            let mut state = self.state.lock().await;
            let next = transition(*state, TransportEvent::ShutdownComplete)?;
            *state = next;
        }

        Ok(())
    }
}
