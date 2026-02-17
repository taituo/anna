# TODO - Session Findings and Next Actions

## Session Snapshot
- Date: 2026-02-17
- Latest scanner: `/Users/tiny/Documents/projects2/anna/testtrack-rust/target/release/testtrack-rust scan . --ci`
- Current status: `3 failed, 17 passed, 241 total issues`

## What Was Completed In This Session
- Vault provider modularized into focused modules (http/auth/ops/wire/config/store/parse/render/tests split).
- Workflow module split (`workflow_tests.rs`, `workflow_support.rs`) and API surface tightened.
- Executor API surface reduced (builder/accessor methods moved to `pub(crate)`).
- Executor impl split into smaller impl blocks to remove `impl has 29 methods` modularity issue.
- Functional checks remain stable: `funcquality=0`, `shadow=0`, `security=0`, `duplication=0`.
- All tests passing: `cargo test -q`.

## Commits Pushed During This Session
- `a3105cb` refactor vault provider into focused modules
- `669b6c2` split vault orchestration into parse, file, render, and types modules
- `2222aec` move workflow tests to dedicated module file
- `fae022d` extract workflow validation and duration helpers into support module
- `cf34903` reduce workflow public API surface to internal methods
- `e713415` limit executor builder methods to crate-internal visibility
- `2028226` split executor impl into focused method blocks

## Open Findings (From Latest Scan)

### 1) Modularity (22 issues)
- `src/daemon.rs:1` file is 6343 lines (max 300)
- `src/daemon.rs:33` struct `AppState` has 17 fields (max 8)
- `src/daemon.rs:54` struct `SessionInfo` has 9 fields (max 8)
- `src/daemon.rs:87` struct `PolicyResponse` has 21 fields (max 8)
- `src/daemon.rs:175` struct `WorkflowMetaResponse` has 22 fields (max 8)
- `src/daemon.rs:214` struct `FlowCheckResponse` has 14 fields (max 8)
- `src/daemon.rs:288` struct `ChatIntentCheckResponse` has 16 fields (max 8)
- `src/daemon.rs:339` struct `HitlPending` has 9 fields (max 8)
- `src/daemon.rs:825` struct `StartupLogContext` has 9 fields (max 8)
- `src/daemon.rs:4167` struct `WorkflowEntry` has 15 fields (max 8)
- `src/daemon.rs:1` file has 201 functions (max 15)
- `src/daemon.rs:8` 10 imports from `axum` (max 6)
- `src/daemon.rs:1` god module warning
- `src/executor.rs:1` file is 2127 lines (max 300)
- `src/executor.rs:1` file has 46 functions (max 15)
- `src/executor.rs:1` god module warning
- `src/workflow.rs:27` struct `Stage` has 39 fields (max 8)
- `src/workflow.rs:144` struct `Workflow` has 10 fields (max 8)
- `src/main.rs:1` file is 1920 lines (max 300)
- `src/main.rs:1` file has 43 functions (max 15)
- `src/mcp.rs:1` file is 1780 lines (max 300)
- `src/mcp.rs:1` file has 69 functions (max 15)

### 2) Test Coverage Heuristic (32 issues)
- Remaining public-api testcov items are listed in `bugs.md` under `### testcov (32 issues)`.
- Biggest remaining modules in that list: `policy_sync`, `policy_crypto`, `providers/mod.rs`, `executor` (only `new`), `workflow` (`load`), `result`.

### 3) Magic Literals (186 issues)
- Full list is in `bugs.md` under `### magic (186 issues)`.
- Largest hotspots by count:
  - `src/daemon.rs` (91)
  - `src/mcp.rs` (16)
  - `src/executor.rs` (13)
  - `src/main.rs` (12)
  - `src/policy_crypto.rs` (11)

## Prioritized Next Work
1. `daemon.rs` vertical split (routes/models/startup/registry/hitl) to drop largest modularity block.
2. `executor.rs` file split (run-loop, worktree, parallel, hooks, provider-exec).
3. `main.rs` command handlers split by command-group module.
4. `mcp.rs` tool handlers split (`policy`, `flow`, `session`, `hitl`, `rpc`).
5. Batch constants pass for magic literals in high-churn files (`daemon`, `mcp`, `executor`, `main`).

## Notes For Next Session
- Source of truth for raw issue rows: `bugs.md`.
- Keep validating after each chunk:
  - `cargo test -q`
  - `/Users/tiny/Documents/projects2/anna/testtrack-rust/target/release/testtrack-rust scan . --ci`
