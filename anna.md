# Anna DSL Syntax v0.4.0

## Quick Start

```yaml
# Minimal workflow
name: hello
stages:
  - id: greet
    exec: "echo Hello World"
```

```yaml
# LLM workflow
name: review
stages:
  - id: code
    exec: "cat main.go"
  - id: review
    provider: llm
    do: "Review this code: $code"
    needs: [code]
```

```yaml
# Parallel with vote
name: analyze
stages:
  - id: think
    provider: llm
    forks: 3
    do: "Analyze this problem"
    vote: "Pick best analysis"
```

---

## Workflow Structure

```yaml
name: string              # workflow name (required)
mode: once|continuous     # execution mode (default: once)
memory: bool              # persist outputs (default: false)
tags: [strings]           # workflow tags

trigger:                  # auto-start conditions (daemon mode)
  webhook: string         # HTTP path, e.g. /deploy
  watch: string           # file glob, e.g. "*.go"
  cron: string            # cron expression, e.g. "*/5 * * * *"
  interval: duration      # polling interval, e.g. 30s

vars:                     # workflow variables
  KEY: value

env:                      # workflow-level env vars
  KEY: value              # supports ${vault:path}, ${env:VAR}, $var

workdir: string           # default working directory for all stages

stages:
  - id: string            # stage id (required, unique)
    provider: shell|cli|llm|http|k8s  # execution provider (default: shell)
    
    # Shell provider
    exec: string          # shell command
    workdir: string       # working directory
    env:                  # environment variables
      KEY: value

    # CLI provider (native wrapper)
    exec: string          # provider binary or command name
    args: [strings]       # command arguments
    stdin: string         # optional stdin payload
    parse: text|json      # output parsing mode (default: text)
    
    # LLM provider
    do: string            # prompt
    system: string        # system prompt
    model: string         # model override
    context: [files]      # files to include in prompt
    trust: none|read|all  # tool trust level (kiro-cli)
    
    # HTTP provider
    exec: string          # URL (GET) or "POST url"
    
    # Output
    output: string        # save output to file
    
    # Flow control
    needs: [stage_ids]    # dependencies
    when: string          # condition
    timeout: duration     # max execution time
    loop: bool            # repeat stage
    interval: duration    # loop interval
    break_when: string    # stop when output contains this
    max_iterations: int   # hard limit for loops
    
    # Parallel execution
    forks: int            # parallel copies
    models: [strings]     # per-fork model (round-robin)
    each: [strings]       # different input per fork ($each)
    each_from: stage_id   # split stage output as each items (\\n or \\n---\\n)
    vote: string          # judge prompt - pick best fork result
    worktree: string      # git worktree branch name
    
    # Retry
    retry: int            # retry count
    retry_delay: duration # delay between retries
    
    # Hooks
    before: string        # shell command before stage
    after: string         # shell command after stage
    on_error: string      # shell command on error
    
    # Human-in-the-loop
    hitl: bool            # pause for human input
    hitl_prompt: string   # what to ask
    hitl_options: [strings]  # allowed responses
    
    # Sub-workflows
    workflow: string      # path to sub-workflow .anna file
    vars:                 # variables to pass to sub-workflow
      KEY: value
    
    # Secrets
    secrets:              # inject secrets as env vars
      ENV_VAR: vault/path           # bare path → vault (default)
      ENV_VAR: vault://path         # explicit vault
      ENV_VAR: env://SYSTEM_VAR     # read from environment
      ENV_VAR: file:///etc/secret   # read from file
```

## Built-in Variables

| Variable | Description |
|----------|-------------|
| `$SESSION` | Unique run ID (8 hex chars) |
| `$stage_id` | Output from stage |
| `$stage_id.N` | Output from fork N (0-indexed) |
| `$stage_id.all` | All fork/each outputs combined |
| `$each` | Current item in each: loop |
| `$memory.stage` | Previous run output (memory: true) |

## Providers

### shell (default)
```yaml
- id: build
  exec: "make build"
  workdir: /app
  timeout: 5m
```

### cli (recommended wrapper layer)
```yaml
- id: model-review
  provider: cli
  exec: "./tools/anna-llm-provider"
  args: ["--model", "gpt-4o-mini", "--mock"]
  stdin: "Review this code: $code"
  parse: text
```

Detailed contract: see `provider_cli_spec.md`.

### llm
`llm` should be implemented as an adapter/provider wrapper, not hardwired into core runtime.

```yaml
- id: analyze
  provider: llm
  system: "You are a code reviewer."
  do: "Review: $code"
  model: claude-sonnet
  trust: all  # allow tool use
```

### http
```yaml
- id: notify
  provider: http
  exec: "POST https://slack.com/webhook"
```

### k8s (auto-enabled in cluster)
```yaml
- id: heavy-task
  provider: k8s
  exec: "python train.py"
```

## Triggers (daemon mode)

```yaml
name: auto-deploy
trigger:
  webhook: /deploy        # POST /hook/deploy triggers this
  watch: "dist/*.js"      # file change triggers this
  cron: "0 * * * *"       # hourly
  interval: 5m            # every 5 minutes
```

## Conditions

```yaml
# Basic
when: "$var != ''"              # not empty
when: "$var == ''"              # empty  
when: "$var == 'value'"         # equals
when: "$stage.success == true"  # stage succeeded

# Contains (substring match)
when: "$check contains 'FAIL'"  # true if output has FAIL
when: "$check contains 'PASS'"  # true if output has PASS

# Logical operators
when: "$a != '' && $b == 'ok'"  # AND - both must be true
when: "$a == '' || $b == ''"    # OR - either can be true
```

## Parallel Execution

### Forks (same input, multiple runs)
```yaml
- id: review
  provider: llm
  forks: 3
  models: [claude-sonnet, gpt-4o, claude-sonnet]
  do: "Review this code"
  vote: "Pick the best review"  # LLM judges
```

### Each (different input per fork)
```yaml
- id: process
  each: [file1.txt, file2.txt, file3.txt]
  exec: "process $each"
```

### Each from stage output
```yaml
- id: list-files
  exec: "ls *.go"

- id: lint
  each_from: list-files  # splits by \n (or \n---\n for forked outputs)
  exec: "golint $each"
  needs: [list-files]
```

## Human-in-the-Loop

```yaml
- id: review
  provider: llm
  do: "Suggest fix for: $bug"
  hitl: true
  hitl_prompt: "Apply this fix?"
  hitl_options: [yes, no, edit]
```

CLI prompts user, response in `$review.hitl`

## Sub-workflows

```yaml
# parent.anna
name: pipeline
stages:
  - id: build
    exec: "make build"
  
  - id: fix
    workflow: fix-loop.anna
    vars:
      PROJECT: ./src
      MAX_TRIES: 5
    needs: [build]
    when: "$build.success == false"
```

Sub-workflow runs with its own session ID, linked to parent via `_meta.json`.
Parent receives last stage output as `$fix`.

## Hooks

```yaml
- id: deploy
  exec: "kubectl apply -f k8s/"
  before: "echo 'Starting deploy...'"
  after: "slack-notify 'Deployed!'"
  on_error: "slack-notify 'Deploy failed!'"
```

## Memory

When `memory: true`, outputs persist to `~/.anna/memory/WORKFLOW.json`

```yaml
name: monitor
memory: true

stages:
  - id: check
    exec: "curl localhost:8080/health"
  
  - id: analyze
    provider: llm
    do: |
      Current: $check
      Previous: $memory.check
      Is this improving?
```

## Session Logging

Every stage writes raw output to `/tmp/anna/$SESSION/<stage_id>.log`.
For LLM stages, the log contains full kiro-cli output (before banner stripping).

```bash
anna logs              # list recent sessions
anna logs abc123ef     # view logs for session
```

## CLI Usage

```bash
# Run workflows
anna                        # show plays + help
anna workflow.anna          # run workflow
anna run workflow.anna      # explicit run
anna "fix the tests"        # LLM picks workflow

# Daemon
anna daemon                 # start server + UI
anna submit workflow.anna   # submit to daemon
anna status                 # show running workflows
anna stop <session>         # stop workflow
anna logs [session]         # view logs

# Info
anna plays                  # list workflows
anna budget                 # LLM rate limits
anna validate               # check all .anna files

# Flags
anna -var "KEY=val" wf.anna # override variables
anna -model gpt-4o wf.anna  # override model
anna --dry-run wf.anna      # print without running
```

## Runtime Profiles

Anna supports three runtime profiles with the same workflow DSL:

- `single`: one workflow run from CLI, then exit
- `daemon`: long-running server for triggers and API control
- `multi-ha`: multiple daemon nodes with shared control-plane policy

The workflow file format is identical across all three profiles.

## Daemon API

```bash
# Health
curl localhost:8080/health

# List workflows
curl localhost:8080/workflows

# Start workflow
curl -X POST localhost:8080/workflow -d @workflow.anna

# Get workflow status
curl localhost:8080/workflow/SESSION_ID

# Stop workflow
curl -X DELETE localhost:8080/workflow/SESSION_ID

# Trigger webhook
curl -X POST localhost:8080/hook/deploy

# WebSocket logs
wscat -c ws://localhost:8080/ws?id=SESSION_ID
```

## Control and Access Layers

Flows can be accessed via multiple interfaces while keeping one execution contract:

- CLI (`anna run`, `anna submit`, `anna status`, `anna logs`)
- HTTP control API (run, status, stop, hook, log streaming)
- MCP server tools (`list_flows`, `run_flow`, `session_status`, `tail_logs`, `stop_flow`)
- Chat gateways (intent -> approved flow run via control API)

For production governance, use a flow registry keyed by `flow_id` + `path` + `tags` + required capabilities.

## Security and Control Invariants

In all runtime profiles:

- agent workflows can request capacity or provider changes, but cannot self-elevate capability ceiling
- trust/policy ceilings are controlled by master control-plane policy
- critical flows should require HITL gates for apply/deploy/policy mutation stages
- secret resolution should use vault/env/file providers, never hardcoded values

## Provider Failure Semantics

Provider failures should be explicit and machine-readable:

- `provider_not_found`: configured provider binary or command does not exist
- `provider_start_failed`: provider process could not start
- `provider_timeout`: provider exceeded stage timeout
- `provider_invalid_response`: parse mode is `json` but output is invalid
- `provider_exec_failed`: provider returned non-zero execution status

These errors must fail the stage deterministically (no hidden fallback), unless workflow retry policy is configured.

See `architecture_modes.md` for deployment topology and control-plane patterns.

## Examples

### CI/CD Pipeline
```yaml
name: deploy
trigger:
  webhook: /deploy

stages:
  - id: test
    exec: "make test"
    timeout: 5m
  
  - id: build
    exec: "make build"
    needs: [test]
  
  - id: deploy
    exec: "kubectl apply -f k8s/"
    needs: [build]
    after: "slack-notify 'Deployed $SESSION'"
```

### Bug Fix Loop
```yaml
name: fix-loop
mode: continuous
memory: true

stages:
  - id: test
    exec: "go test ./..."
    break_when: "PASS"
    max_iterations: 5
  
  - id: fix
    provider: llm
    trust: all
    do: "Fix this test failure: $test"
    needs: [test]
```

### Parallel Review with Vote
```yaml
name: code-review

stages:
  - id: review
    provider: llm
    forks: 3
    models: [claude-sonnet, gpt-4o, gemini-pro]
    do: "Review this PR for bugs and style issues"
    vote: "Pick the most thorough and accurate review"
```
