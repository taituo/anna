# Anna CLI Provider Spec v0.1

This spec defines a native provider contract for wrapping external binaries (including LLM CLIs) without coupling Anna core to any one toolchain.

## Scope

Use `provider: cli` when:
- calling model wrappers (`openai`, `claude`, local model CLIs)
- running tool adapters that are not first-class native providers
- isolating unstable wrappers from core engine releases

## Stage Shape

```yaml
- id: explain
  provider: cli
  exec: "anna-llm-provider"
  args: ["--model", "gpt-4o-mini"]
  stdin: "Explain: $input"
  parse: text
  timeout: 2m
```

Fields:
- `exec`: binary path or command name (required)
- `args`: CLI arguments (optional)
- `stdin`: payload written to stdin (optional)
- `parse`: `text` or `json` (optional, default `text`)

## Runtime Contract

Input from Anna:
- process args from `args`
- stdin from `stdin` when provided
- stage/workflow context via env (recommended):
  - `ANNA_SESSION`
  - `ANNA_WORKFLOW`
  - `ANNA_STAGE_ID`
  - `ANNA_TRUST`

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
