# Repository Instructions

## Core operation mode

- Do not begin implementation edits without explicit user approval.
- For change requests, first provide a concrete plan and implementation options, then wait for confirmation before writing files.
- Prefer to finish alignment first, then do one-shot editing.

## Interaction policy for this repo

- Default workflow for user requests that imply code changes:
  1. Confirm scope and assumptions.
  2. Propose a concrete patch plan (files, behavior, tradeoffs).
  3. Ask for go-ahead before any `apply_patch`/file edit.
- If the request is “Please implement now”, treat it as approved implementation and proceed.
- If the request is uncertain or broad, pause at planning stage.

## Testing/verification

- Do not run project-wide validation unless explicitly requested.
- Do not introduce validation or tests for quick planning-only edits unless asked.
- Keep changes minimal and scoped to requested files.

## File edit conventions

- Use existing conventions and preserve formatting/style in touched files.
- Favor clear, minimal diffs over broad refactors.
- Prefer ASCII unless existing file contains non-ASCII and requires continuity.

## Notes for runtime/path work

- Always keep user-facing behavior stable when possible.
- For new asynchronous/runtime work, document lifecycle, shutdown, and error handling paths before implementation.
