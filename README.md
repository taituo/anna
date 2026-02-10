# Anna DSL

Anna is a YAML-based workflow language for orchestrating system tasks and LLM-powered agent stages in one pipeline.

This repository contains the language docs:

- `anna.md`: full syntax reference (`v0.4.0`) with examples
- `anna_language.md`: concept and design rationale ("the missing layer")
- `architecture_modes.md`: runtime and control-plane architecture (single, daemon, multi-HA)
- `provider_cli_spec.md`: native provider contract for CLI wrappers (including LLM adapters)
- `tools/README.md`: reference wrapper usage (`tools/anna-llm-provider`)

## What Anna Supports

- Deterministic stages (`shell`, `http`, `k8s`)
- Native `cli` provider wrappers for external tools and model CLIs
- LLM stages as first-class workflow providers (`llm`) decoupled from core
- Dependencies and conditions (`needs`, `when`)
- Triggers in daemon mode (`webhook`, `watch`, `cron`, `interval`)
- Parallel execution (`forks`, `each`, `each_from`) with optional `vote`
- Human-in-the-loop approvals (`hitl`)
- Sub-workflows (`workflow`)
- Persistent workflow memory (`memory: true`)

## Minimal Example

```yaml
name: hello
stages:
  - id: greet
    exec: "echo Hello World"
```

## CLI Snapshot

```bash
anna validate
anna run workflow.anna
anna daemon
```

## Rust Runtime (MVP)

This repository now includes a Rust runtime foundation in `src/`:

- `workflow` parsing/validation for `.anna` YAML
- core substitution and `when` evaluation
- provider registry with `shell`, `cli`, `http`, `llm`, `k8s` (LLM and k8s via CLI adapters)
- executor with `needs`, `when`, retry, timeout, hooks, and session logs
- session metadata files (`/tmp/anna/<session>/_meta.json`) with parent/child linkage
- daemon API scaffold (`health`, workflow submit/status/stop/logs, local workflow listing, basic `/ws?id=...` stream)

Run locally:

```bash
cargo run -- validate botbet.anna
cargo run -- run botbet.anna --max-iterations 1
```

Current MVP intentionally leaves some advanced features for next steps (richer daemon scheduling/HA and production-grade live log streaming semantics).
`forks`, `each/each_from`, `vote`, sub-workflows, and memory persistence are implemented in the Rust executor foundation.

## Runtime Profiles

- `single`: run one flow directly and exit
- `daemon`: long-running node with API/webhook/log streaming
- `multi-ha`: multiple daemon nodes under shared control-plane policy

## Access Channels

- CLI (`anna run`, `anna submit`, `anna status`)
- HTTP control API (`/workflow`, `/hook/*`, `/ws`)
- MCP tools (`list_flows`, `run_flow`, `session_status`, `tail_logs`, `stop_flow`)
- Chat gateway (maps chat intents to approved flow runs)

For complete syntax and advanced examples, read `anna.md` first, then `anna_language.md`.
For runtime/control-plane design, read `architecture_modes.md`.

## License

MIT (see `LICENSE`).
