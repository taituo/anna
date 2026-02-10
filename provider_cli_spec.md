# Anna CLI Provider Spec v0.1

This spec defines a native provider contract for wrapping external binaries (including LLM CLIs) without coupling Anna core to any one toolchain.

## Scope

Use `provider: cli` when:
- calling model wrappers (`openai`, `claude`, local model CLIs)
- running tool adapters that are not first-class native providers
- isolating unstable wrappers from core engine releases

Reference implementation in this repository:
- `tools/anna-llm-provider`

## Stage Shape

```yaml
- id: explain
  provider: cli
  exec: "./tools/anna-llm-provider"
  args: ["--model", "gpt-4o-mini", "--mock"]
  stdin: "Explain: $input"
  parse: text
  timeout: 2m
```

Fields:
- `exec`: binary path or command name (required)
- `args`: CLI arguments (optional)
- `stdin`: payload written to stdin (optional)
- `parse`: `text` or `json` (optional, default `text`)

## Quick Start

```bash
# deterministic smoke test
./tools/anna-llm-provider --model gpt-4o-mini --mock --prompt "hello"

# JSON output mode
./tools/anna-llm-provider --model gpt-4o-mini --mock --format json --prompt "hello"

# real backend via env
ANNA_LLM_BACKEND_CMD=cat ./tools/anna-llm-provider --model local --prompt "ping"
```

## Runtime Contract

Input from Anna:
- process args from `args`
- stdin from `stdin` when provided
- stage/workflow context via env:
  - `ANNA_SESSION`
  - `ANNA_WORKFLOW`
  - `ANNA_STAGE_ID`
  - `ANNA_TRUST`

Current Rust runtime injects these `ANNA_*` variables automatically for `provider: cli` and `provider: shell`.
When `stage.secrets` is set, mapped env vars are also injected (resolved from `ANNA_SECRET_*` or `~/.anna/secrets.json`).

Output to Anna:
- `parse: text` -> full stdout captured as stage output
- `parse: json` -> stdout must be valid JSON object

## Failure Semantics (Required)

CLI providers must fail with explicit reason classes:

- `provider_not_found`: command not present in PATH / file missing
- `provider_start_failed`: process spawn error
- `provider_timeout`: timeout reached
- `provider_invalid_response`: invalid JSON when `parse: json`
- `provider_exec_failed`: command ran but returned failure

No silent fallback is allowed. Errors must remain deterministic and auditable.

Exit codes:
- `10` -> `provider_not_found`
- `11` -> `provider_start_failed`
- `12` -> `provider_timeout`
- `13` -> `provider_invalid_response`
- `14` -> `provider_exec_failed`

## LLM as Adapter (Recommended)

`provider: llm` is logically supported, but implementation should be adapter-based:
- llm stage -> adapter -> `provider: cli` wrapper
- model/provider switching should happen in adapter config, not Anna core

This keeps model churn and CLI quirks out of the main execution engine.

## MCP and CLI Best Practices

- keep request/response contract stable and versioned
- use structured logs to stderr, structured payload to stdout
- avoid interactive prompts in provider process
- expose health/version flags (`--version`, `--help`)
- return fast, typed errors instead of ambiguous text

## Skills Integration

If a CLI wrapper has task-specific behavior, include a `SKILL.md` alongside it:
- capabilities
- expected input schema
- output schema
- failure modes and recovery hints

This helps Anna agents reason about wrappers consistently across flows.
