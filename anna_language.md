# Anna: The Missing Layer

## What Anna Is

Anna is a low-code DSL for orchestrating LLM-powered agents. It's programmable through YAML—no SDKs, no MCPs, no framework lock-in. The LLM is just another provider, callable via API or CLI. Workflows are single executable files or long-running daemons. Memory persists across runs to support stateful chatbots. HITL (human-in-the-loop) stages can pause for approval, connect to Jira tickets, or create GitLab issues for voting. Everything is pluggable. Everything is declarative.

Anna runs both deterministic and non-deterministic setups. Shell stages execute predictably every time. LLM stages introduce controlled randomness—multiple models can vote on the best answer, or a single model can iterate until tests pass. The same workflow file works for strict CI pipelines and exploratory AI agents. You choose the execution model per stage.

There's a gap in modern DevOps tooling that nobody talks about. GitLab CI excels at triggering pipelines when code changes, but it can't think. Ansible automates infrastructure tasks beautifully, but it follows scripts blindly. Between these two worlds—between "something happened" and "do these exact steps"—lies a space where intelligent decision-making should live. Anna was built to fill that gap.

Anna is a workflow orchestration language designed specifically for LLM-powered agents operating at the system level. It speaks YAML because infrastructure teams already think in YAML. It integrates with HashiCorp Vault because secrets management isn't optional in production environments. It runs shell commands, calls LLM APIs, makes HTTP requests, and spawns Kubernetes jobs—all from the same declarative workflow definition. The language treats AI models as first-class execution providers, not afterthoughts bolted onto existing tools.

The core insight behind Anna is simple: modern systems need agents that can observe, reason, and act autonomously. A CI pipeline can detect that tests failed, but it cannot analyze the failure, hypothesize a fix, implement it, and verify the solution. Anna workflows can. They loop continuously, maintain memory across iterations, fork into parallel execution paths, and use voting mechanisms to select the best outcome from multiple AI-generated solutions. This isn't automation—it's orchestrated intelligence.

Consider what happens when a production alert fires at 3 AM. Traditional tools page an engineer who must wake up, understand the context, diagnose the issue, and implement a fix. An Anna workflow can receive that alert via webhook, gather system metrics, analyze logs with an LLM, propose remediation steps, execute them with appropriate trust levels, and verify the fix—all while maintaining an audit trail. The engineer reviews the session logs in the morning, not the raw alert.

Anna workflows are composable by design. A parent workflow can invoke child workflows, passing variables and receiving results. Session IDs link parent and child executions together, creating traceable trees of autonomous actions. This enables building complex multi-stage pipelines where each stage might itself be a sophisticated AI-driven process. The fix-loop workflow calls the code-review workflow which spawns three parallel analysis workers—all tracked, all logged, all reproducible.

The trust model in Anna reflects real-world security requirements. LLM stages can be granted no tool access, read-only access, or full system access. Secrets are injected from Vault, environment variables, or encrypted files—never hardcoded. Workflows run in isolated git worktrees when needed, preventing cross-contamination between parallel execution branches. These aren't features added for compliance checkboxes; they're fundamental to running AI agents in production environments where mistakes have consequences.

Parallelism in Anna goes beyond simple fan-out patterns. The `each` directive runs a stage once per input item concurrently. The `forks` directive creates multiple copies of the same stage with different models. The `vote` directive adds a judge LLM that evaluates all parallel outputs and selects the best one. Combined, these primitives enable sophisticated ensemble approaches: three different AI models analyze the same problem, a fourth model picks the winner, and the workflow continues with the consensus answer.

Memory persistence transforms Anna from a stateless executor into a learning system. When memory is enabled, each workflow run can access outputs from previous runs. An LLM analyzing code can see what bugs it found yesterday and avoid reporting duplicates. A monitoring workflow can compare current metrics against historical baselines. This temporal awareness is essential for agents that operate continuously rather than in isolated bursts.

The daemon mode exposes Anna as an HTTP service with WebSocket support for real-time log streaming. Workflows can be triggered by webhooks, cron schedules, file system changes, or direct API calls. A GitLab CI pipeline can POST a workflow to the Anna daemon when tests fail, delegating the intelligent remediation to a specialized system while the CI runner moves on. This separation of concerns—CI handles triggering, Anna handles thinking—reflects how modern infrastructure should be architected.

Anna exists because the industry needed a language that treats LLMs as infrastructure components rather than chat interfaces. It bridges the gap between event-driven automation and intelligent response, between "run this script" and "solve this problem." The YAML syntax is familiar, the execution model is predictable, the security model is production-ready, and the AI integration is native. For teams building autonomous systems that must operate reliably at scale, Anna provides the orchestration layer that was missing.

---

## Anna as Master Control

Anna is the control plane. Providers are replaceable execution backends under Anna policy.

That means:

- Changing provider internals does not change workflow semantics.
- The abstraction layer remains stable even when engines evolve (Go now, Rust later, mixed runtime, etc.).
- Agent workflows can evolve themselves only within bounded capability ceilings.
- LLM integration can be replaced independently as a provider adapter (preferably via CLI wrapper contract).

This is the key to safe autonomy: the system can iterate, but policy remains above the agent.

## Runtime Modes

Anna should run in three modes without forking the language:

- Single mode: run one flow deterministically and exit.
- Daemon mode: long-running service with trigger and API support.
- Multi-HA mode: distributed daemon nodes under shared policy.

The same `.anna` workflow must execute consistently in all three modes.

## Access Model (CLI, API, MCP, Chat)

Anna is not only a CLI. Enterprise usage needs multiple access channels:

- CLI for operators.
- HTTP API for systems and automation.
- MCP tools for LLM-native agent interfaces.
- Chat gateway for human command and approval workflows.

All channels should map into one control contract so auditing, policy, and replay remain uniform.

## Kubernetes Optional, Not Required

Anna can run with Kubernetes, but it must also operate without Kubernetes in edge or small deployments.

A single-node daemon or VM/container runtime should still be able to talk to master control securely, execute deterministic tasks, and continue with safe fallback policy when disconnected.

This keeps Anna useful from startup-scale single instance deployments to geo-distributed enterprise fleets.

---

## Milestone

The ultimate validation of Anna as a language is self-improvement: an Anna workflow that enhances Anna itself.

```yaml
name: self-improve
mode: continuous
memory: true

stages:
  - id: test
    exec: "cd /path/to/anna && go test ./..."
    break_when: "PASS"
    max_iterations: 10

  - id: analyze
    provider: llm
    trust: all
    system: "You are improving the Anna workflow engine. Read the codebase."
    do: |
      Test results: $test
      Previous improvements: $memory.analyze
      
      Find ONE bug or improvement opportunity. Implement it.
    needs: [test]

  - id: verify
    exec: "cd /path/to/anna && go test ./..."
    needs: [analyze]
```

When this workflow runs successfully—when Anna can read its own source code, identify improvements, implement them, and verify the changes pass all tests—the language will have achieved its design goal. Not artificial general intelligence, but practical autonomous improvement within a bounded domain. A tool that makes itself better.

That milestone is already reached.

Soft AGI? Maybe not, but with Anna your application can be maintaned, improved and feature developed.

