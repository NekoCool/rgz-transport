# Staged Dependency Upgrade Plan

This document defines the staged dependency upgrade sequence for the workspace:

1. `rgz-derive`
2. `rgz-msgs`
3. `rgz-transport`

The order is intentional. `rgz-msgs` depends on `rgz-derive`, and `rgz-transport` depends on `rgz-msgs`.

## Goals

- Keep each stage small and reviewable.
- Detect regressions early with crate-scoped checks before workspace-wide checks.
- Avoid mixed root-cause failures by not upgrading all crates at once.

## Baseline Before Any Upgrade

Run and record baseline on current `main`:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p rgz-transport --lib
```

Optional snapshot for comparison:

```bash
cargo tree -p rgz-derive > /tmp/tree-derive.before.txt
cargo tree -p rgz-msgs > /tmp/tree-msgs.before.txt
cargo tree -p rgz-transport > /tmp/tree-transport.before.txt
```

## Stage 1: Upgrade `rgz-derive`

Scope:

- `crates/rgz-derive/Cargo.toml`
- lockfile updates caused by this crate

Candidate dependencies:

- `syn`
- `quote`
- `regex`

Validation:

```bash
cargo check -p rgz-derive
cargo clippy -p rgz-derive --all-targets -- -D warnings
cargo test -p rgz-derive
```

Exit criteria:

- No API/behavioral change in generated derive output.
- `rgz-msgs` still compiles with new `rgz-derive`.

## Stage 2: Upgrade `rgz-msgs`

Scope:

- `crates/rgz-msgs/Cargo.toml`
- lockfile updates caused by `prost` ecosystem changes

Candidate dependencies:

- `prost`
- `prost-types`
- `prost-build`

Validation:

```bash
cargo check -p rgz-msgs
cargo clippy -p rgz-msgs --all-targets -- -D warnings
cargo check -p rgz-transport
```

Additional checks:

- Confirm generated code flow is unchanged (`build.rs` still emits expected `gz.msgs.rs` shape).
- Confirm `rgz-msgs` public message types remain compatible with `rgz-transport` usage.

Exit criteria:

- `rgz-msgs` builds cleanly.
- Downstream compile (`rgz-transport`) remains green.

## Stage 3: Upgrade `rgz-transport`

Scope:

- `crates/rgz-transport/Cargo.toml`
- lockfile updates for runtime stack

Candidate dependencies:

- `tokio`
- `uuid`
- `socket2`
- `local-ip-address`
- `once_cell`
- `regex`
- `tracing` / `tracing-subscriber`
- `whoami`
- `anyhow`
- `async-trait`
- `prost` / `prost-types` (if still directly used)
- `futures` / `chrono` (dev-dependencies)

Validation:

```bash
cargo check -p rgz-transport
cargo clippy -p rgz-transport --all-targets -- -D warnings
cargo test -p rgz-transport --lib
```

If network tests are enabled in CI:

```bash
cargo test -p rgz-transport --lib --features network-tests -- --ignored --test-threads=1
```

Exit criteria:

- Unit tests pass.
- No new panic paths or transport/discovery regressions.

## Final Workspace Gate (After Each Stage Merge)

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p rgz-transport --lib
```

## Rollback Strategy

- Keep one PR per stage.
- If a stage regresses:
  - Revert only that stage PR.
  - Keep previous stage(s) merged.
  - Retry with narrower dependency increments.

## Suggested PR Sequence

1. `chore(deps): upgrade rgz-derive dependencies`
2. `chore(deps): upgrade rgz-msgs prost stack`
3. `chore(deps): upgrade rgz-transport runtime dependencies`
