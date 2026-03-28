# Packaging & Setup Review — Prof. Dr. Kai Bruckner (DevOps)

**Date:** 2026-03-19
**Scope:** Installation, distribution, Docker integration

---

## Environment Analysis

| Tool | Status | Version |
|------|--------|---------|
| `uv` | Available | 0.9.26 |
| `pip` | Available | 24.0 (system) |
| `pipx` | Not installed | — |
| Python | Available | 3.12.3 (`/usr/bin/python3`) |
| MCP SDK | Already installed | 1.12.4 |
| httpx, pydantic, jinja2, pyyaml, click | All available | Current versions |

All core dependencies are already installed in system Python.

---

## Recommendations

### 1. Installation: Docker Container

> **Update:** After discussion, the decision is to run Brix as a Docker container, consistent with the existing infrastructure where everything runs in Docker.

Docker Compose setup with:
- Docker socket mounting for `docker exec -i` to reach other MCP servers
- `~/.brix` volume for persistent config
- Wrapper script at `/usr/local/bin/brix` for transparent access

### 2. Build System: Hatchling

Use Hatchling over setuptools — modern, fast, native `uv` support. No legacy baggage.

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

Version pinning: Only `>=` as lower bound. `uv.lock` handles reproducibility. `pyproject.toml` defines compatibility.

### 3. CLI: Click (not Typer)

Click gives more direct control for complex command hierarchies (`brix run`, `brix server add`, `brix server tools`, `brix history`, etc.). Typer adds magic that complicates debugging.

**Async + Click:** Click is synchronous. Use `asyncio.run()` as the bridge:

```python
@click.command()
def run(pipeline, query):
    result = asyncio.run(engine.run(pipeline, query=query))
    click.echo(json.dumps(result))
```

### 4. History: SQLite (not YAML)

`brix history` and `brix stats` need queries. SQLite (`~/.brix/history.db`) is vastly more robust than YAML files — and it's in Python's stdlib. No new dependency.

### 5. MCP Server Communication via `docker exec -i`

For container-based MCP servers, `docker exec -i` provides transparent stdio bridging:

```yaml
# servers.yaml
m365:
  command: docker
  args: ["exec", "-i", "m365-mcp-container", "node", "/app/index.js"]
```

stdin/stdout flows transparently. Brix doesn't need to know whether a server runs in a container or not — same `command` + `args` syntax in `servers.yaml`.

Connection pooling works correctly with `docker exec -i`: the process stays open as long as Brix holds the stdio connection.

### 6. Workdir Cleanup Strategy

- Auto-delete after successful run (default)
- Keep 24h after failure (for `--resume` and debugging)
- `brix clean --older-than 24h` for manual cleanup

### 7. Runtime Directories

```
~/.brix/
├── servers.yaml          # MCP server registry
├── pipelines/            # User pipelines
├── helpers/              # Python helper scripts
├── cache/
│   └── schemas/          # MCP tool schema cache
└── history.db            # SQLite run history
```

`/tmp/brix/<run-id>/` for workdirs — cleaned up per policy above.
