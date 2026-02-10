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
- Secret injection (`stage.secrets`) with env/file resolution
- Dependencies and conditions (`needs`, `when`)
- Triggers in daemon mode (`webhook`, `watch`, `cron`, `interval`)
- Parallel execution (`forks`, `each`, `each_from`) with optional `vote` (concurrent runtime)
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
anna mcp
anna submit workflow.anna
anna workflows
anna workflows-meta
anna workflows-meta --available true --capability k8s
anna can-run prod-deploy
anna run-named prod-deploy --var ENV=prod --max-iterations 1
anna hook deploy
anna status <request_id>
anna sessions --status running
anna stats
anna wait <request_id> --timeout-sec 300
anna hitl list
anna hitl list --status pending --workflow my-flow
```

Optional daemon auth:

```bash
export ANNA_DAEMON_TOKEN=your-token
anna daemon
anna status <request_id>  # CLI sends Bearer token automatically
```

Daemon state persistence (sessions + HITL):

```bash
# default: ~/.anna/daemon-state.json
anna daemon

# custom path or disable
ANNA_DAEMON_STATE_FILE=/var/lib/anna/state.json anna daemon
ANNA_DAEMON_STATE_FILE=off anna daemon
```

Daemon retention limits (in-memory + persisted snapshots):

```bash
ANNA_DAEMON_MAX_SESSIONS=5000 anna daemon
ANNA_DAEMON_MAX_HITL=2000 anna daemon
```

Optional flow registry (restrict daemon-discoverable flows to approved list):

```bash
ANNA_FLOW_REGISTRY_FILE=./flows.registry.yml anna daemon --plays-dir .
```

Optional node capability ceiling (used against `required_capabilities`):

```bash
ANNA_NODE_CAPABILITIES="shell,http,k8s,vault" anna daemon --plays-dir .
```

Registry format:

```yaml
flows:
  - flow_id: prod-deploy
    path: deploy.anna
    tags: [prod, deploy]
    required_capabilities: [k8s]
    max_concurrency: 2
    owner: platform
    version: v1
```

When registry is enabled:
- `anna workflows` lists `flow_id` values
- `anna workflows-meta` shows owner/version/tags/capability availability (supports `--tag`, `--owner`, `--capability`, `--available`, `--limit`)
- `anna run-named <name>` accepts `flow_id`, workflow name, or file name
- `anna run-named` accepts optional runtime JSON options (`vars`, `max_iterations`)
- hook/cron/watch/interval trigger scans are limited to registry entries
- flows with missing `required_capabilities` are skipped/blocked with explicit reason
- optional `max_concurrency` caps simultaneous runs for named/manual runs and trigger launches

## Rust Runtime (MVP)

This repository now includes a Rust runtime foundation in `src/`:

- `workflow` parsing/validation for `.anna` YAML
- core substitution and `when` evaluation
- provider registry with `shell`, `cli`, `http`, `llm`, `k8s` (LLM and k8s via CLI adapters)
- executor with `needs`, `when`, retry, timeout, hooks, `stage.loop`/`break_when`/`max_iterations`, session logs, and `stage.worktree` git isolation
- session metadata files (`/tmp/anna/<session>/_meta.json`) with parent/child linkage
- daemon API + trigger scheduler (`health`, workflow submit/status/stop/logs, `/hook/*`, `/hitl/*`, `/ws`, plus `trigger.interval|cron|watch`)

Run locally:

```bash
cargo run -- validate botbet.anna
cargo run -- run botbet.anna --max-iterations 1
```

Current MVP intentionally leaves some advanced features for next steps (multi-node HA coordination and production-grade live log streaming semantics).
`forks`, `each/each_from`, `vote`, sub-workflows, and memory persistence are implemented in the Rust executor foundation.

## Runtime Profiles

- `single`: run one flow directly and exit
- `daemon`: long-running node with API/webhook/log streaming
- `multi-ha`: multiple daemon nodes under shared control-plane policy

## Access Channels

- CLI (`anna run`, `anna submit`, `anna can-run`, `anna status`)
- HTTP control API (`/workflow`, `/workflow/{name}/check`, `/workflow/{name}/run`, `/workflows`, `/workflows/meta`, `/hook/*`, `/hitl`, `/hitl/{id}/resolve`, `/ws`)
- MCP stdio server (`anna mcp`) with tools: `list_flows`, `list_flows_meta`, `run_flow`, `can_run_flow`, `session_status`, `tail_logs`, `stop_flow`, `list_sessions`, `stats`, `trigger_hook`, `list_hitl`, `resolve_hitl`
- Chat gateway (maps chat intents to approved flow runs)

Minimal MCP smoke test:

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  | anna mcp
```

For complete syntax and advanced examples, read `anna.md` first, then `anna_language.md`.
For runtime/control-plane design, read `architecture_modes.md`.

## License

MIT (see `LICENSE`).
