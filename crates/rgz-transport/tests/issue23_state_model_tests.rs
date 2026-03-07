use rgz_transport::{
    TransportError, TransportEvent, TransportState, state, state::StateModel, transition,
};

#[test]
fn issue23_invalid_transition_is_rejected() {
    assert!(matches!(
        transition(TransportState::Stopped, TransportEvent::StartOk),
        Err(TransportError::InvalidTransition { .. })
    ));
    assert!(matches!(
        transition(TransportState::Stopped, TransportEvent::ShutdownComplete),
        Err(TransportError::InvalidTransition { .. })
    ));
    assert!(matches!(
        transition(TransportState::Created, TransportEvent::StartOk),
        Err(TransportError::InvalidTransition { .. })
    ));
}

#[test]
fn issue23_rejected_transition_does_not_change_state() {
    let mut model = state::StateModel::new(state::RecoveryPolicy {
        recoverable_error_threshold: 3,
        recoverable_window_ms: 5_000,
        recoveries_to_running: 3,
    });

    let original = model;
    assert!(matches!(
        model.apply(TransportEvent::StartOk, 100),
        Err(TransportError::InvalidTransition { .. })
    ));
    assert_eq!(model, original);
}

#[test]
fn issue23_event_ordering_prefers_shutdown_when_timestamp_collides() {
    let model = state::StateModel::new(state::RecoveryPolicy {
        recoverable_error_threshold: 3,
        recoverable_window_ms: 5_000,
        recoveries_to_running: 3,
    });
    let model = {
        let mut started = model;
        started.apply(TransportEvent::InitRequested, 0).unwrap();
        started.apply(TransportEvent::StartOk, 1).unwrap();
        started
    };

    let model = state::apply_events(
        model,
        vec![
            state::TimedEvent::new(100, TransportEvent::ShutdownRequested),
            state::TimedEvent::new(100, TransportEvent::ShutdownComplete),
        ],
    )
    .expect("shutdown should be applied before normal events at same time");

    assert_eq!(model.state, TransportState::Stopped);
}

#[test]
fn issue23_running_to_degraded_to_running_with_retries() {
    let policy = state::RecoveryPolicy {
        recoverable_error_threshold: 4,
        recoverable_window_ms: 5_000,
        recoveries_to_running: 2,
    };
    let mut model = state::StateModel::new(policy);

    model.apply(TransportEvent::InitRequested, 0).unwrap();
    model.apply(TransportEvent::StartOk, 1).unwrap();
    model.apply(TransportEvent::RecoverableError, 10).unwrap();
    assert_eq!(model.state, TransportState::Degraded);

    model.apply(TransportEvent::RetrySucceeded, 20).unwrap();
    assert_eq!(model.state, TransportState::Degraded);

    model.apply(TransportEvent::RetrySucceeded, 30).unwrap();
    assert_eq!(model.state, TransportState::Running);
    assert_eq!(model.recoverable_error_count, 0);
}

#[test]
fn issue23_recoverable_error_can_flow_to_failed() {
    let policy = state::RecoveryPolicy {
        recoverable_error_threshold: 2,
        recoverable_window_ms: 5_000,
        recoveries_to_running: 3,
    };
    let mut model = state::StateModel::new(policy);

    model.apply(TransportEvent::InitRequested, 0).unwrap();
    model.apply(TransportEvent::StartOk, 1).unwrap();
    model.apply(TransportEvent::RecoverableError, 200).unwrap();
    assert_eq!(model.state, TransportState::Degraded);

    model.apply(TransportEvent::RecoverableError, 300).unwrap();
    assert_eq!(model.state, TransportState::Failed);
}

#[test]
fn issue23_transition_matrix_forbidden_transitions() {
    let forbidden = [
        (TransportState::Stopped, TransportEvent::StartOk),
        (TransportState::Stopped, TransportEvent::StartErr),
        (TransportState::Stopped, TransportEvent::RecoverableError),
        (TransportState::Stopped, TransportEvent::FatalError),
        (TransportState::Stopped, TransportEvent::RetrySucceeded),
        (TransportState::Stopped, TransportEvent::ShutdownComplete),
        (TransportState::Stopped, TransportEvent::ShutdownRequested),
        (TransportState::Running, TransportEvent::StartOk),
        (TransportState::Running, TransportEvent::StartErr),
        (TransportState::Running, TransportEvent::ShutdownComplete),
        (TransportState::Stopped, TransportEvent::InitRequested),
        (TransportState::Created, TransportEvent::StartOk),
        (TransportState::Created, TransportEvent::StartErr),
        (TransportState::Created, TransportEvent::RecoverableError),
        (TransportState::Created, TransportEvent::FatalError),
        (TransportState::Created, TransportEvent::RetrySucceeded),
        (TransportState::Created, TransportEvent::ShutdownComplete),
        (TransportState::Starting, TransportEvent::RecoverableError),
        (TransportState::Starting, TransportEvent::FatalError),
        (TransportState::Starting, TransportEvent::RetrySucceeded),
        (TransportState::Starting, TransportEvent::ShutdownComplete),
        (TransportState::Degraded, TransportEvent::StartErr),
        (TransportState::Degraded, TransportEvent::StartOk),
        (TransportState::Degraded, TransportEvent::InitRequested),
        (TransportState::Stopping, TransportEvent::InitRequested),
        (TransportState::Stopping, TransportEvent::StartOk),
        (TransportState::Stopping, TransportEvent::StartErr),
        (TransportState::Stopping, TransportEvent::RecoverableError),
        (TransportState::Stopping, TransportEvent::FatalError),
        (TransportState::Stopping, TransportEvent::RetrySucceeded),
        (TransportState::Failed, TransportEvent::InitRequested),
        (TransportState::Failed, TransportEvent::StartOk),
        (TransportState::Failed, TransportEvent::StartErr),
        (TransportState::Failed, TransportEvent::RecoverableError),
        (TransportState::Failed, TransportEvent::RetrySucceeded),
    ];

    for (from, event) in forbidden {
        assert!(
            matches!(
                transition(from, event),
                Err(TransportError::InvalidTransition { .. })
            ),
            "{from:?} -> {event:?} should be forbidden"
        );
    }
}

#[test]
fn issue23_state_model_rejects_forbidden_transitions_without_mutating_state() {
    let mut model = StateModel::default();

    for (state, event) in [
        (TransportState::Stopped, TransportEvent::StartOk),
        (TransportState::Created, TransportEvent::StartErr),
        (TransportState::Starting, TransportEvent::RetrySucceeded),
    ] {
        model.state = state;
        let before = model;
        assert!(
            matches!(
                model.apply(event, 1234),
                Err(TransportError::InvalidTransition { .. })
            ),
            "{state:?} -> {event:?} should be invalid"
        );
        assert_eq!(
            model, before,
            "{state:?} -> {event:?} should not mutate state"
        );
    }
}
