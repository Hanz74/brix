# Spec Review — Alex (Requirements Engineer)

**Date:** 2026-03-19
**Scope:** Full architecture review of Brix

---

## CRITICAL

### C-1: Jinja2 Template Injection via user-controlled input

Pipeline parameters like `--query "invoices from last week"` land directly in Jinja2 templates: `filter: "{{ input.query }}"`. Jinja2 executes arbitrary expressions. A user could pass `{{ import('os').system('rm -rf /') }}` as a query.

**Missing:** No mention of sandboxing (Jinja2 `SandboxedEnvironment`). The current architecture assumes input is safe.

**Action required:** Define which Jinja2 environment is used. `jinja2.sandbox.SandboxedEnvironment` should be mandatory. Clarify whether Jinja2 expressions in input parameters are even allowed, or only in the YAML definition itself.

### C-2: CLI Runner allows arbitrary code execution without scope

The `cli` runner executes shell commands: `command: "markitdown '{{ item.path }}'"`. Since `item.path` comes from a previous step (which may process HTTP responses), shell injection is possible if the path isn't sanitized. Example: a filename `"; rm -rf ~; echo "` would pass through to the shell.

**Missing:** No specification for argument escaping in the CLI runner. No indication whether `subprocess` runs with `shell=True` or `shell=False` + args list.

**Action required:** CLI commands must be specifiable as args lists, not just strings. This must be defined in the pipeline format.

### C-3: Workdir cleanup after resume is undefined

The workdir `/tmp/brix/<run-id>/` is used for resume. Lifecycle is unclear: when is it cleaned up? On systemd systems `/tmp` is cleaned on reboot, but on a long-running server `/tmp` can persist for weeks. One workdir per run means unbounded growth without an explicit cleanup policy.

**Missing:** No retention policy for workdirs. No `brix cleanup` command in CLI reference.

---

## HIGH

### H-1: foreach + parallel with on_error: continue — output schema breaks

With `foreach: ... parallel: true` and `on_error: continue`, some items succeed, others fail. The next step references `{{ download.output }}` — but what is the output when 3 of 7 items failed?

**Missing:** The output schema for partial-success foreach is not defined. This is the most common real-world case.

### H-2: Sub-pipelines — input/output contract missing

How is the sub-pipeline's `result` field mapped into the parent context? Is `batch_download.output` the sub-pipeline's `result` object? The entire JSON response? Only `steps`?

Additionally: Is the sub-pipeline executed as a separate process or in the same asyncio loop? If separate: MCP connection pooling breaks (pools are process-local).

### H-3: MCP server startup time vs. connection pooling scope

Connection pooling is "for the pipeline's lifetime". But sub-pipelines might be separate processes. Also: what happens on `--resume`? The original MCP process is gone.

**Missing:** Resume + MCP connection lifecycle is unspecified.

### H-4: Credentials handling — BRIX_CRED_* is too primitive for OAuth

The credential system assumes tokens already exist in ENV. For M365 (OAuth2): token refresh is the user's responsibility. Token expiry during a pipeline is not handled.

**Missing:** No token refresh mechanism, no credential type system, no specification for 401 handling in the HTTP runner.

### H-5: Dry-run — what gets "mocked"?

`brix run --dry-run` is in the CLI reference but not specified. Three fundamentally different interpretations exist.

**Missing:** The definition of dry-run is completely absent.

### H-6: `when` condition — scope and type unclear

- Is a skipped step listed as `skipped` in the output JSON or missing entirely?
- Can subsequent steps reference the output of a skipped step? Exception or empty object?
- Is `when` a Jinja2 boolean expression or just a variable reference?

### H-7: Python Runner — subprocess vs importlib — two completely different security models

`subprocess`: isolated, no shared state, but overhead.
`importlib`: same Python process, shares memory, can affect Brix internals, no isolation.

**Missing:** The decision which model is used (or when which) is missing. Massive implications for security, performance, and error isolation.

---

## MEDIUM

| # | Issue |
|---|-------|
| M-1 | `brix_version` constraint enforcement — `>=`, `==`, or SemVer range? |
| M-2 | `foreach` without `parallel` — what is the default? |
| M-3 | Jinja2 rendering timing — eager vs lazy |
| M-4 | Test mocking — which steps run, which are mocked? |
| M-5 | Snapshot tests — storage, invalidation, initial creation |
| M-6 | Output size budget — when does Brix switch to file references? |
| M-7 | Missing `output` field in pipeline format |
| M-8 | Step-level parallelization (DAG) — not just foreach |
| M-9 | History storage — where and how? |

## LOW

| # | Issue |
|---|-------|
| L-1 | `brix list bricks` — what are "bricks" exactly? |
| L-2 | `brix info <pipeline>` — what exactly is shown? |
| L-3 | Skill prompt args parsing convention |
| L-4 | `brix validate` vs `brix test` — overlapping responsibilities |
| L-5 | Concurrent modifications to `~/.brix/` |

---

## Summary

| Severity | Count | Core theme |
|----------|-------|------------|
| CRITICAL | 3 | Security (injection), workdir lifecycle |
| HIGH | 7 | Data flow contracts, MCP lifecycle, credentials |
| MEDIUM | 9 | Missing format fields, unclear semantics |
| LOW | 5 | CLI inconsistencies, conventions |

**Verdict:** Solid architecture with clear scope. The token savings logic is compelling. Critical gaps lie in **security** (injection risks), **data flow contracts for partial failures**, and **missing execution model decisions** for sub-pipelines and the Python runner. These must be documented as explicit decisions before implementation starts.
