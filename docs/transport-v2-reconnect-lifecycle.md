# Transport v2 reconnect lifecycle

`TxCmd::Connect` and `TxCmd::Disconnect` are actor commands. The actor owns a mutable endpoint
configuration, rather than allowing socket tasks to retain independent endpoint state.

## Lifecycle

1. `Disconnect` removes the endpoint from the runtime's PUB, SUB, and DEALER connection lists.
2. The actor stops the SUB and DEALER receive tasks and drops the PUB and DEALER send halves.
3. The actor recreates the runtime from the updated configuration and restores all subscriptions.
4. `Connect` adds the endpoint to the same connection lists and repeats the rebuild.

The endpoint-only public command deliberately applies to all client socket roles because it has no
socket-role parameter. Bind endpoints remain immutable construction-time configuration.

## Errors and shutdown

Socket creation or subscription restoration failures are emitted as retryable transport errors and
move the state machine to `Degraded`. A later successful connect, send, or receive transitions the
state back to `Running`. Shutdown uses the same task-stop path and does not leave receive tasks
running after the actor exits.
