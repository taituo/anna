# Anna DSL

Anna is a YAML-based workflow language for orchestrating system tasks and LLM-powered agent stages in one pipeline.

This repository contains the language docs:

- `anna.md`: full syntax reference (`v0.4.0`) with examples
- `anna_language.md`: concept and design rationale ("the missing layer")

## What Anna Supports

- Deterministic stages (`shell`, `http`, `k8s`)
- LLM stages as first-class workflow providers (`llm`)
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

For complete syntax and advanced examples, read `anna.md` first, then `anna_language.md`.

## License

MIT (see `LICENSE`).
