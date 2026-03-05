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
