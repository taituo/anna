# Tools

## anna-llm-provider

Reference CLI wrapper for Anna `provider: cli` integration.

Path:
- `tools/anna-llm-provider`

Examples:

```bash
# Mock mode (deterministic)
./tools/anna-llm-provider --model gpt-4o-mini --mock --prompt "hello"

# JSON mode
./tools/anna-llm-provider --model gpt-4o-mini --mock --format json --prompt "hello"

# External backend (stdin -> backend stdin)
ANNA_LLM_BACKEND_CMD=cat ./tools/anna-llm-provider --model local --prompt "ping"
```

Common flags:
- `--model <id>` (required)
- `--format text|json` (default `text`)
- `--backend-cmd <command>` (optional if `ANNA_LLM_BACKEND_CMD` is set)
- `--backend-arg <arg>` (repeatable)
- `--timeout-seconds <n>`
- `--mock`
- `--prompt <text>` (if omitted, reads stdin)
