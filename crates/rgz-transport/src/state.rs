use std::fmt;

use crate::error::TransportError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportState {
    Created,
    Starting,
    Running,
    Degraded,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportEvent {
    InitRequested,
    StartOk,
    StartErr,
    RecoverableError,
    FatalError,
    RetrySucceeded,
    ShutdownRequested,
    ShutdownComplete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryPolicy {
    pub recoverable_error_threshold: u32,
    pub recoverable_window_ms: u64,
    pub recoveries_to_running: u32,
}

impl Default for RecoveryPolicy {
    fn default() -> Self {
        Self {
            recoverable_error_threshold: 3,
            recoverable_window_ms: 5_000,
            recoveries_to_running: 3,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateModel {
    pub state: TransportState,
    pub recoverable_error_count: u32,
    pub recoverable_first_ms: Option<u64>,
    pub retry_success_count: u32,
    pub last_recovered_at: Option<u64>,
    pub policy: RecoveryPolicy,
}

impl Default for StateModel {
    fn default() -> Self {
        Self {
            state: TransportState::Created,
            recoverable_error_count: 0,
            recoverable_first_ms: None,
            retry_success_count: 0,
            last_recovered_at: None,
            policy: RecoveryPolicy::default(),
        }
    }
}

impl StateModel {
    pub fn new(policy: RecoveryPolicy) -> Self {
        Self {
            policy,
            ..Self::default()
        }
    }

    pub fn apply(&mut self, event: TransportEvent, at_ms: u64) -> Result<(), TransportError> {
        use TransportEvent::*;
        use TransportState::*;

        self.state = match (self.state, event) {
            (Created, InitRequested) => {
                self.recoverable_error_count = 0;
                self.recoverable_first_ms = None;
                self.retry_success_count = 0;
                self.last_recovered_at = None;
                Starting
            }
            (Starting, StartOk) => {
                self.recoverable_error_count = 0;
                self.recoverable_first_ms = None;
                self.retry_success_count = 0;
                Running
            }
            (Starting, StartErr) => Stopped,
            (Running, RecoverableError) => {
                self.recoverable_error_count = 1;
                self.recoverable_first_ms = Some(at_ms);
                self.retry_success_count = 0;
                Degraded
            }
            (Running, FatalError) => Failed,
            (Degraded, RecoverableError) => {
                let base = self.recoverable_first_ms.unwrap_or(at_ms);
                let window_ok = at_ms.saturating_sub(base) <= self.policy.recoverable_window_ms;
                self.recoverable_error_count = if window_ok {
                    self.recoverable_error_count.saturating_add(1)
                } else {
                    1
                };
                self.recoverable_first_ms = Some(base);
                self.last_recovered_at = Some(at_ms);

                if self.recoverable_error_count >= self.policy.recoverable_error_threshold {
                    Failed
                } else {
                    Degraded
                }
            }
            (Degraded, RetrySucceeded) => {
                self.retry_success_count = self.retry_success_count.saturating_add(1);
                if self.retry_success_count >= self.policy.recoveries_to_running {
                    self.recoverable_error_count = 0;
                    self.recoverable_first_ms = None;
                    self.retry_success_count = 0;
                    self.last_recovered_at = Some(at_ms);
                    Running
                } else {
                    Degraded
                }
            }
            (Degraded, FatalError) => Failed,
            (Running, ShutdownRequested) | (Degraded, ShutdownRequested) | (Failed, ShutdownRequested) => {
                self.recoverable_error_count = 0;
                self.recoverable_first_ms = None;
                self.retry_success_count = 0;
                Stopping
            }
            (Stopping, ShutdownComplete) => Stopped,
            (state, event) => {
                return Err(TransportError::InvalidTransition { from: state, event });
            }
        };

        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TimedEvent {
    pub at_ms: u64,
    pub event: TransportEvent,
}

impl TimedEvent {
    pub const fn new(at_ms: u64, event: TransportEvent) -> Self {
        Self { at_ms, event }
    }
}

fn event_priority(event: TransportEvent) -> u8 {
    if matches!(event, TransportEvent::ShutdownRequested) {
        0
    } else {
        1
    }
}

pub fn apply_events(
    mut model: StateModel,
    events: impl IntoIterator<Item = TimedEvent>,
) -> Result<StateModel, TransportError> {
    let mut list: Vec<_> = events.into_iter().collect();

    list.sort_by(|a, b| {
        a.at_ms
            .cmp(&b.at_ms)
            .then_with(|| event_priority(a.event).cmp(&event_priority(b.event)))
    });

    for e in list {
        model.apply(e.event, e.at_ms)?;
    }

    Ok(model)
}

impl fmt::Display for TransportState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransportState::*;
        let label = match self {
            Created => "Created",
            Starting => "Starting",
            Running => "Running",
            Degraded => "Degraded",
            Stopping => "Stopping",
            Stopped => "Stopped",
            Failed => "Failed",
        };
        write!(f, "{label}")
    }
}

impl fmt::Display for TransportEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransportEvent::*;
        let label = match self {
            InitRequested => "InitRequested",
            StartOk => "StartOk",
            StartErr => "StartErr",
            RecoverableError => "RecoverableError",
            FatalError => "FatalError",
            RetrySucceeded => "RetrySucceeded",
            ShutdownRequested => "ShutdownRequested",
            ShutdownComplete => "ShutdownComplete",
        };
        write!(f, "{label}")
    }
}

pub fn transition(
    current: TransportState,
    event: TransportEvent,
) -> Result<TransportState, TransportError> {
    use TransportEvent::*;
    use TransportState::*;

    let next = match (current, event) {
        (Created, InitRequested) => Starting,
        (Starting, StartOk) => Running,
        (Starting, StartErr) => Stopped,
        (Running, RecoverableError) => Degraded,
        (Running, FatalError) => Failed,
        (Running, ShutdownRequested) => Stopping,
        (Degraded, RecoverableError) => Degraded,
        (Degraded, RetrySucceeded) => Running,
        (Degraded, FatalError) => Failed,
        (Degraded, ShutdownRequested) => Stopping,
        (Failed, ShutdownRequested) => Stopping,
        (Stopping, ShutdownComplete) => Stopped,
        (state, event) => return Err(TransportError::InvalidTransition { from: state, event }),
    };

    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::{
        apply_events, event_priority, StateModel, TimedEvent, TransportError, TransportEvent::*,
        TransportState::*, RecoveryPolicy,
    };

    #[test]
    fn transition_table_is_complete_for_issue_scope() {
        assert_eq!(super::transition(Created, InitRequested).unwrap(), Starting);
        assert_eq!(super::transition(Starting, StartOk).unwrap(), Running);
        assert_eq!(super::transition(Starting, StartErr).unwrap(), Stopped);
        assert_eq!(super::transition(Running, RecoverableError).unwrap(), Degraded);
        assert_eq!(super::transition(Running, FatalError).unwrap(), Failed);
        assert_eq!(super::transition(Running, ShutdownRequested).unwrap(), Stopping);
        assert_eq!(super::transition(Degraded, RetrySucceeded).unwrap(), Running);
        assert_eq!(super::transition(Degraded, ShutdownRequested).unwrap(), Stopping);
        assert_eq!(super::transition(Failed, ShutdownRequested).unwrap(), Stopping);
        assert_eq!(super::transition(Stopping, ShutdownComplete).unwrap(), Stopped);
    }

    #[test]
    fn invalid_transitions_are_rejected() {
        assert!(matches!(
            super::transition(Stopped, StartOk),
            Err(TransportError::InvalidTransition { .. })
        ));
        assert!(matches!(
            super::transition(Created, StartErr),
            Err(TransportError::InvalidTransition { .. })
        ));
        assert!(matches!(
            super::transition(Failed, StartOk),
            Err(TransportError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn state_model_tracks_recovery_to_failed_after_recoverable_errors() {
        let policy = RecoveryPolicy {
            recoverable_error_threshold: 3,
            recoverable_window_ms: 5_000,
            recoveries_to_running: 3,
        };
        let mut model = StateModel::new(policy);

        model.apply(InitRequested, 0).unwrap();
        model.apply(StartOk, 1).unwrap();
        model.apply(RecoverableError, 100).unwrap();
        assert_eq!(model.state, Degraded);

        model.apply(RecoverableError, 200).unwrap();
        assert_eq!(model.state, Degraded);
        assert_eq!(model.recoverable_error_count, 2);

        model.apply(RecoverableError, 300).unwrap();
        assert_eq!(model.state, Failed);
        assert_eq!(model.recoverable_error_count, 3);
    }

    #[test]
    fn state_model_returns_running_after_consecutive_retries() {
        let policy = RecoveryPolicy {
            recoverable_error_threshold: 3,
            recoverable_window_ms: 5_000,
            recoveries_to_running: 3,
        };
        let mut model = StateModel::new(policy);

        model.apply(InitRequested, 0).unwrap();
        model.apply(StartOk, 1).unwrap();
        model.apply(RecoverableError, 100).unwrap();
        model.apply(RetrySucceeded, 200).unwrap();
        assert_eq!(model.state, Degraded);
        assert_eq!(model.retry_success_count, 1);

        model.apply(RetrySucceeded, 300).unwrap();
        assert_eq!(model.retry_success_count, 2);
        assert_eq!(model.state, Degraded);

        model
            .apply(RetrySucceeded, 500)
            .expect("three recoveries should return to running");
        assert_eq!(model.state, Running);
        assert_eq!(model.retry_success_count, 0);
    }

    #[test]
    fn shutdown_request_is_prioritized_when_timestamps_match() {
        let mut model = StateModel::default();
        model.apply(InitRequested, 0).unwrap();
        model.apply(StartOk, 1).unwrap();

        let events = vec![
            TimedEvent::new(100, ShutdownRequested),
            TimedEvent::new(100, ShutdownComplete),
        ];

        let model = apply_events(model, events).unwrap();
        assert_eq!(model.state, Stopped);
    }

    #[test]
    fn event_priority_places_shutdown_first() {
        let mut list = vec![
            TimedEvent::new(10, RetrySucceeded),
            TimedEvent::new(10, ShutdownRequested),
            TimedEvent::new(10, RecoverableError),
        ];

        list.sort_by(|a, b| {
            a.at_ms
                .cmp(&b.at_ms)
                .then_with(|| event_priority(a.event).cmp(&event_priority(b.event)))
        });
        assert_eq!(list[0].event, ShutdownRequested);
    }
}
