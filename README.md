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

## Examples

Transport examples live under `crates/rgz-transport/examples`.

```bash
cargo run -p rgz-transport --example publisher
cargo run -p rgz-transport --example subscriber
```
