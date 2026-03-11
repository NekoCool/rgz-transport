# rgz-transport

`rgz-transport` and its required internal crates are maintained in this repository:

- package `rgz-transport` (`crates/rgz-transport`)
- package `rgz-msgs` (`crates/rgz-msgs`)
- package `rgz-derive` (`crates/rgz-derive`)

`crates/rgz_sim` has been removed from the active workspace.

## Workspace

This repository uses a virtual workspace at the root:

```toml
[workspace]
members = [
  "crates/rgz-derive",
  "crates/rgz-msgs",
  "crates/rgz-transport",
]
default-members = ["crates/rgz-transport"]
```

## Setup

`rgz-msgs` depends on the `gz-msgs` git submodule for `.proto` files.

```bash
git submodule update --init --recursive
```

## Build

```bash
cargo check --workspace
```

## Testing

Run the default unit suite:

```bash
cargo test -p rgz-transport --lib
```

Run network integration tests (ignored by default):

```bash
cargo test -p rgz-transport --lib --features network-tests -- --ignored --test-threads=1
```

## API error handling

`rgz-transport` exposes explicit backpressure/error signaling.
`TransportHandle` methods below return `Result` and should be handled by callers:

- `send_cmd`
- `publish`
- `request`
- `reply`
- `subscribe`
- `unsubscribe`
- `connect`
- `disconnect`
- `shutdown_cmd`

Default channel policy:

| Queue path | Default capacity | Overflow behavior |
| --- | ---: | --- |
| `command` | `1024` | `Err(TransportError::NodeBusy { path: "command" })` |
| `control` | `128` | `Err(TransportError::NodeBusy { path: "control" })` |
| `event` | `2048` | `DropNewest` (existing queued events are preserved) |
| `io_event` | `2048` | `DropNewest` (existing queued events are preserved) |
| `sub_cmd` | `512` | error event (`subscribe/unsubscribe command failed`) |

Queue saturation counters are tracked in-memory and exposed via `TransportHandle::metrics_snapshot()`.

Backpressure handling example:

```rust
match handle.publish("topic/a", payload, None).await {
    Ok(()) => {}
    Err(rgz_transport::TransportError::NodeBusy { .. }) => {
        // retry with backoff/jitter
    }
    Err(err) => return Err(err),
}
```

## Examples

Transport examples live under `crates/rgz-transport/examples`.

```bash
cargo run -p rgz-transport --example publisher
cargo run -p rgz-transport --example subscriber
```

## Performance Benchmark (MVP)

`rgz-transport` contains a Criterion benchmark with:

- throughput measurement (`criterion`)
- latency percentile measurement (`hdrhistogram`, p50/p95/p99)
- runtime observability (`tracing`, `metrics`)

Run:

```bash
cargo bench -p rgz-transport --bench transport_perf
```

The benchmark publishes and subscribes in-process over transport and tests
message sizes `64`, `1024`, and `8192` bytes.
