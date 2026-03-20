# Deployment + Security Review Brix v2 — Bruckner (DevOps) + SecOps

**Date:** 2026-03-20

## Bruckner: Deployment

### stdio transport first, HTTP only when needed
- stdio via `docker exec -i brix brix-mcp` — no ports, no TLS
- HTTP/SSE only for multi-client scenarios (v2.2+)
- Two docker-compose profiles: `brix` (CLI) + `brix-mcp` (MCP server)

### Lazy Connection Pooling for MCP servers
- Register servers at startup (metadata only)
- Connect on first tool call, hold connection
- Reconnect on EOF/crash — this is mandatory, not nice-to-have

### Resource limits
- Memory: 512MB limit (generous for asyncio + MCP connections)
- CPU: 1.0 (I/O-bound, not CPU-bound)
- Concurrent runs: Semaphore(1) for v2.0, async tasks for v2.1

## SecOps: Security

### Auth model is correct for stdio
- MCP servers handle own auth (OAuth, API keys) — Brix never sees tokens
- stdio = implicit auth via Docker socket access
- HTTP transport would need API keys, TLS, IP allowlist

### Critical: Three high-privilege capabilities

**Docker Socket:** Full host compromise possible. Mitigation:
- `allowed_containers` allowlist in config
- Only `docker exec`, never `docker run`

**CLI Runner:** Command injection risk. Mitigation:
- P0: `shell=False` always, argument lists not strings
- P1: `command` field must be static (no Jinja2 templates)
- P1: `allowed_executables` allowlist optional

**Python Runner:** No secure inline eval exists. Mitigation:
- P0: Remove `python_eval` inline — only `python_script` with file paths
- Scripts in container, not in pipeline YAML

### Jinja2 Sandbox
- SandboxedEnvironment prevents template injection — keep it
- But rendered values must never be used as shell strings
- Two defense layers: sandbox + shell=False

### Recommendations

| Priority | Measure |
|----------|---------|
| P0 | shell=False enforced in CLI runner |
| P0 | No inline python_eval — only python_script |
| P0 | Reconnect logic for MCP stdio bridges |
| P1 | allowed_containers allowlist |
| P1 | Timeout per brick (no hangs) |
| P1 | command field: no Jinja2 templates |
| P2 | Memory limit 512M |
| P2 | Document: stdio-only, no HTTP without auth review |
| defer | Rate limiting, permission system, HTTP transport |

### Bottom Lines

**Bruckner:** stdio is the right starting point. Risks (reconnect, memory) are solvable.

**SecOps:** Brix is a privileged system tool by design. Risks are manageable
with shell=False + no inline eval + allowlists. Docker socket access is the
one fundamental risk that can't be abstracted away — document, accept, constrain.
