# Anna Runtime and Control Modes

This document defines how Anna runs in three operating modes while keeping one DSL and one execution model.

## Goals

- Same workflow semantics in all modes (`needs`, `when`, `retry`, `timeout`, `hitl`, `memory`)
- Same provider abstraction even when implementations change
- Deterministic core for critical system operations
- Strict capability ceiling so agents cannot self-escalate

## Runtime Profiles

### 1) Single Mode

Use when one workflow is run directly from CLI.

```bash
anna run flow.anna
```

Characteristics:
- Local execution
- Minimal dependencies
- Good for development and deterministic jobs

### 2) Daemon Mode

Use when workflows need triggers and continuous API access.

```bash
anna daemon
```

Characteristics:
- Long-running service
- HTTP/WebSocket control and logs
- Supports webhook, watch, cron, interval triggers

### 3) Multi-HA Mode

Use when multiple nodes must share execution load with high availability.

Characteristics:
- Shared control policy from MasterControl
- Leader election and distributed execution
- Consistent audit and session tracking across nodes

## MasterControl Model

Anna is the master control plane. Providers and workers are managed components under Anna policy.

Rules:
- Agent workflows can request changes, but cannot raise their own capability ceiling
- Policy, trust ceilings, budgets, and approvals are controlled above worker level
- Critical actions can require HITL even in autonomous loops

## Access Channels to Flows

Flows should be reachable from multiple interfaces through one control contract.

1. CLI
- Local operator entrypoint for run/submit/status/logs

2. HTTP Control API
- Primary runtime control surface (`run`, `status`, `stop`, `logs`, `hook`)

3. MCP Server
- Tool interface for LLM agents
- Typical tools: `list_flows`, `run_flow`, `session_status`, `tail_logs`, `stop_flow`

4. Chat Gateway
- Slack/Discord/Telegram/web chat adapter
- Maps approved intent -> flow id + vars -> control API call

## Flow Registry

Use a registry instead of unrestricted filesystem scanning.

Minimum fields:
- `flow_id`
- `path`
- `tags`
- `required_capabilities`
- `owner`
- `version`

This enables clear governance over which flows can be executed by whom.

## Provider Abstraction Contract

Provider internals may change, but the contract must remain stable.

Input:
- normalized stage context
- vars/env
- trust and capability scope

Output:
- `status`: success/fail/timeout
- `stdout` / `artifacts`
- structured metadata (`duration`, `retry_count`, `provider`)

LLM integration should be adapter-based:
- core runtime should not be tightly coupled to a single model SDK/CLI
- `llm` can map internally to a `cli` provider wrapper
- provider wrappers can evolve independently from Anna core

## CLI Provider Baseline

Use `cli` as the native wrapper surface for external binaries, model CLIs, and MCP-compatible tools.

Baseline requirements:
- explicit command path/name and args
- predictable stdin/stdout contract
- structured exit reason on failure (`provider_not_found`, `provider_timeout`, etc.)
- graceful crash behavior (clear error, no silent `not found` ambiguity)

This keeps provider development fast while preventing wrapper instability from blocking core engine work.

## Deterministic Core + Pluggable Providers

Keep deterministic operations in engine-native code (Rust-friendly path):
- state machine transitions
- policy checks
- retry/timeout logic
- patch/apply/verify orchestration

Use providers for external execution domains:
- shell
- cli
- llm (prefer implemented via cli wrapper)
- http
- k8s

## Deployment Without Kubernetes

Anna must run also outside Kubernetes for edge and single-instance deployments.

Recommended stack:
- `systemd` or `docker compose` for process lifecycle
- mTLS connection to MasterControl
- local fallback policy snapshot for control-plane outages
- strict offline mode that allows only pre-approved deterministic flows

## Geo-Redundant Pattern

Use a control-plane aware geo model:
- region-local workers
- central or replicated MasterControl policy authority
- asynchronous event aggregation and audit shipping
- explicit capacity request flow for scaling events

## Enterprise Guardrails

- Capability ceiling (non-bypassable)
- Budget and concurrency limits per tenant/persona
- Secret access only via Vault/OpenBao with short-lived credentials
- Immutable audit trail for decisions, diffs, approvals, rollbacks
- Rollback contract required for high-risk flows
