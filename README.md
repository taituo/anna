# Anna DSL

Anna is a YAML-based workflow language for orchestrating system tasks and LLM-powered agent stages in one pipeline.

This repository contains the language docs:

- `anna.md`: full syntax reference (`v0.4.0`) with examples
- `anna_language.md`: concept and design rationale ("the missing layer")
- `architecture_modes.md`: runtime and control-plane architecture (single, daemon, multi-HA)
- `provider_cli_spec.md`: native provider contract for CLI wrappers (including LLM adapters)
- `tools/README.md`: reference wrapper usage (`tools/anna-llm-provider`)
- `llm.adapters.example.yml`: starter LLM adapter catalog template

## What Anna Supports

- Deterministic stages (`shell`, `http`, `k8s`, `vault`)
- Native `cli` provider wrappers for external tools and model CLIs
- LLM stages as first-class workflow providers (`llm`) decoupled from core
- LLM adapter catalog (`ANNA_LLM_ADAPTERS_FILE`) for model/CLI wrapper routing
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
anna can-chat deploy --caller ops-bot --max-iterations 2
anna can-run-yaml workflow.anna
anna run-named prod-deploy --var ENV=prod --max-iterations 1
anna run-named prod-deploy --precheck
anna hook deploy
anna chat-intents
anna chat deploy --caller ops-bot --var ENV=prod
anna status <request_id>
anna sessions --status running
anna sessions --status running --owner platform --workflow prod-deploy
anna stats
anna policy
anna policy-revision
anna policy-snapshot
anna policy-sync
anna policy-verify
anna llm-adapters
anna llm-adapters --json
anna llm-adapters --daemon http://127.0.0.1:8080
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

Daemon policy snapshot persistence (effective control policy):

```bash
# writes effective policy JSON atomically every ~2s
ANNA_POLICY_SNAPSHOT_FILE=/var/lib/anna/policy.snapshot.json anna daemon
ANNA_POLICY_SNAPSHOT_FILE=off anna daemon
```

Optional policy revision signing key (adds HMAC signature to policy endpoints and snapshots):

```bash
ANNA_POLICY_SIGNING_KEY=replace-with-long-random-secret anna daemon
```

Verify signed policy revision from client side:

```bash
# explicit key
anna policy-verify --key replace-with-long-random-secret

# env fallback (preferred for automation)
ANNA_POLICY_VERIFY_KEY=replace-with-long-random-secret anna policy-verify

# do not fail if daemon returns unsigned policy revision
anna policy-verify --allow-unsigned
```

Conditional policy fetch (ETag with revision hash):

```bash
# cache validation
curl -H 'If-None-Match: "REVISION_HASH"' localhost:8080/policy/revision

# strong precondition (rejects with 412 if revision changed)
curl -H 'If-Match: "REVISION_HASH"' localhost:8080/policy/snapshot
```

Policy snapshot sync to local cache (revision-safe + atomic write):

```bash
# default output: ~/.anna/policy.snapshot.json
anna policy-sync

# custom output path
anna policy-sync --output ./state/policy.snapshot.json

# reduce retry budget for revision races (default 3)
anna policy-sync --retries 1

# verify snapshot signature before writing
anna policy-sync --verify --key replace-with-long-random-secret

# in verify mode, tolerate unsigned snapshots
anna policy-sync --verify --allow-unsigned
```

`policy-sync` uses `If-None-Match` against `/policy/revision` and `If-Match` against `/policy/snapshot`.
This prevents stale writes when policy changes mid-fetch and writes the local file via temp+rename.

Daemon retention limits (in-memory + persisted snapshots):

```bash
ANNA_DAEMON_MAX_SESSIONS=5000 anna daemon
ANNA_DAEMON_MAX_HITL=2000 anna daemon
```

Optional flow registry (restrict daemon-discoverable flows to approved list):

```bash
ANNA_FLOW_REGISTRY_FILE=./flows.registry.yml anna daemon --plays-dir .
```

Optional chat gateway intent map (intent -> registered workflow id/name):

```bash
ANNA_CHAT_INTENTS="deploy=prod-deploy,triage=incident-triage" anna daemon --plays-dir .

# file-based mapping (YAML map or list)
ANNA_CHAT_INTENTS_FILE=./chat.intents.yml anna daemon --plays-dir .

# optional hot-reload poll interval for file-based intents (default 2s, off|false|0 disables)
ANNA_CHAT_INTENTS_FILE=./chat.intents.yml ANNA_CHAT_INTENTS_RELOAD_SEC=5 anna daemon --plays-dir .
```

`ANNA_CHAT_INTENTS` overrides keys from `ANNA_CHAT_INTENTS_FILE` when both are set.

Optional trigger leader lease for multi-node daemon deployments (shared filesystem):

```bash
ANNA_DAEMON_NODE_ID=node-a \
ANNA_TRIGGER_LEASE_FILE=/shared/anna-trigger-lease.json \
ANNA_TRIGGER_LEASE_TTL_SEC=15 \
anna daemon --plays-dir .
```

When trigger leader lease is enabled, `POST /hook/{name}` is accepted only on the current leader node.
Follower nodes return `409 Conflict` to prevent duplicate webhook-triggered launches.

`chat.intents.yml` example:

```yaml
deploy:
  workflow: prod-deploy
  allowed_callers: [ops-bot]
  allowed_owners: [platform]
  required_tags: [prod]
  max_iterations_cap: 2
triage: incident-triage
```

Guardrails are enforced in both `anna can-chat` and `anna chat` (`allowed_callers`, `allowed_owners`, `required_tags`, `max_iterations_cap`).
Caller is read from `x-anna-caller` (fallback `x-anna-role`) header when using HTTP/MCP.

Optional node capability ceiling (used against `required_capabilities`):

```bash
ANNA_NODE_CAPABILITIES="shell,http,k8s,vault" anna daemon --plays-dir .
```

Optional provider allowlist ceiling (applies to `run` and daemon-triggered runs):

```bash
ANNA_ALLOWED_PROVIDERS="shell,cli,http" anna daemon --plays-dir .
```

Strict offline mode (deterministic provider ceiling for edge/single-node operation):

```bash
# effective provider ceiling: shell,cli,vault
ANNA_OFFLINE_MODE=true anna daemon --plays-dir .

# explicit allowlist is still capped by offline ceiling
ANNA_OFFLINE_MODE=true ANNA_ALLOWED_PROVIDERS="shell,http" anna daemon --plays-dir .
```

Native vault provider storage config (optional):

```bash
ANNA_VAULT_KV_FILE=~/.anna/vault-kv.json anna run flow.anna
ANNA_VAULT_PREFIX_ALLOW="kv/prod/,kv/shared/" anna daemon --plays-dir .
ANNA_VAULT_READ_ONLY=true anna daemon --plays-dir .
```

Native vault provider HTTP/OpenBao backend (optional):

```bash
# token auth
ANNA_VAULT_BACKEND=http \
ANNA_VAULT_ADDR=http://127.0.0.1:8200 \
ANNA_VAULT_TOKEN=... \
ANNA_VAULT_MOUNT=secret \
ANNA_VAULT_KV_VERSION=2 \
anna run flow.anna

# AppRole auth (token-free)
ANNA_VAULT_BACKEND=http \
ANNA_VAULT_ADDR=http://127.0.0.1:8200 \
ANNA_VAULT_ROLE_ID=... \
ANNA_VAULT_SECRET_ID=... \
ANNA_VAULT_AUTH_PATH=auth/approle/login \
ANNA_VAULT_MOUNT=secret \
ANNA_VAULT_KV_VERSION=2 \
anna run flow.anna
```

Optional owner concurrency policy (per `owner` in registry):

```bash
ANNA_OWNER_MAX_CONCURRENCY="platform=4,research=2,*=1" anna daemon --plays-dir .
```

Optional append-only audit log (NDJSON, one JSON event per line):

```bash
ANNA_AUDIT_LOG_FILE=/var/log/anna/audit.ndjson anna daemon --plays-dir .
```

Emitted events include daemon startup, workflow launch/finish, stop requests, chat intent launch/block, webhook trigger outcomes, trigger leadership transitions (`trigger_leader_acquired` / `trigger_leader_lost`), and HITL resolutions.

Optional LLM adapter catalog (provider-independent wrapper routing):

```bash
cp llm.adapters.example.yml llm.adapters.yml
ANNA_LLM_ADAPTERS_FILE=./llm.adapters.yml ANNA_LLM_ADAPTER=openbao anna run flow.anna
```

```yaml
default: mock
adapters:
  mock:
    exec: ./tools/anna-llm-provider
    args: ["--mock"]
    model: gpt-4o-mini
  openbao:
    exec: ./tools/anna-llm-provider
    args: ["--backend-cmd", "openbao-cli"]
    model: claude-sonnet
    env:
      ANNA_LLM_BACKEND_CMD: openbao-cli
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
- `anna workflows-meta` shows owner/version/tags plus capability/concurrency availability (supports `--tag`, `--owner`, `--capability`, `--available`, `--limit`)
- `anna run-named <name>` accepts `flow_id`, workflow name, or file name
- `anna run-named` accepts optional runtime options (`vars`, `max_iterations`, `--precheck`)
- hook/cron/watch/interval trigger scans are limited to registry entries
- flows with missing `required_capabilities` are skipped/blocked with explicit reason
- flows with blocked `required_providers` (from `ANNA_ALLOWED_PROVIDERS`) are skipped/blocked with explicit reason
- optional `max_concurrency` caps simultaneous runs for named/manual runs and trigger launches
- optional owner policy (`ANNA_OWNER_MAX_CONCURRENCY`) caps total running sessions per owner

## Rust Runtime (MVP)

This repository now includes a Rust runtime foundation in `src/`:

- `workflow` parsing/validation for `.anna` YAML
- core substitution and `when` evaluation
- provider registry with `shell`, `cli`, `http`, `llm`, `k8s`, `vault` (LLM and k8s via CLI adapters)
- executor with `needs`, `when`, retry, timeout, hooks, `stage.loop`/`break_when`/`max_iterations`, session logs, and `stage.worktree` git isolation
- session metadata files (`/tmp/anna/<session>/_meta.json`) with parent/child linkage
- daemon API + trigger scheduler (`health`, workflow submit/status/stop/logs, `/hook/*`, `/hitl/*`, `/ws`, plus `trigger.interval|cron|watch`)
- optional append-only daemon audit log (`ANNA_AUDIT_LOG_FILE`) for operational events

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

- CLI (`anna run`, `anna submit`, `anna can-run`, `anna can-chat`, `anna can-run-yaml`, `anna status`, `anna policy`, `anna policy-revision`, `anna policy-snapshot`, `anna policy-sync`, `anna policy-verify`, `anna chat-intents`, `anna chat`)
- HTTP control API (`/policy`, `/policy/revision`, `/policy/snapshot`, `/llm/adapters`, `/workflow`, `/workflow/check`, `/workflow/{name}/check`, `/workflow/{name}/run`, `/workflows`, `/workflows/meta`, `/chat/intents`, `/chat/{intent}/check`, `/chat/run`, `/hook/*`, `/hitl`, `/hitl/{id}/resolve`, `/ws`)
- MCP stdio server (`anna mcp`) with tools: `list_flows`, `list_flows_meta`, `run_flow`, `can_run_flow`, `can_run_flow_yaml`, `can_run_chat_intent`, `session_status`, `tail_logs`, `stop_flow`, `list_sessions`, `stats`, `policy`, `policy_revision`, `policy_snapshot`, `policy_sync`, `policy_verify`, `list_llm_adapters`, `daemon_llm_adapters`, `trigger_hook`, `list_chat_intents`, `run_chat_intent`, `list_hitl`, `resolve_hitl`
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
