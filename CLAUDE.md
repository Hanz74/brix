# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Was ist Brix?

**Brix** ist ein DB-First Pipeline-Orchestrator fuer Claude Code. Alles lebt in `brix.db` -- Pipelines, Helpers, Bricks, Connectors, Tools, Help Topics. Workflows werden aus Bricks zusammengesteckt -- konfigurieren statt coden. MCP-Server mit stdio + SSE Transport.

**Warum:** Jeder Tool-Call in Claude Code kostet Kontext-Tokens. `brix run` macht viele Calls zu einem. Token-Einsparung: ~99%.

**Aktuelle Zahlen:** `brix --version`, `brix list pipelines`, `brix list helpers`, `mcp__brix__list_bricks()`. Keine hardcodierten Counts hier -- die Tools liefern den aktuellen Stand.

## MCP Server (empfohlen)

Brix ist als MCP Server registriert. Claude sieht `mcp__brix__*` Tools automatisch.

### WICHTIG: Brick-First Regeln

- **KEIN YAML manuell schreiben** -> `mcp__brix__create_pipeline` oder `mcp__brix__compose_pipeline`
- **KEINE Helper-Scripts fuer Standardaufgaben** -> Built-in Bricks: `flow.filter`, `flow.transform`, `llm.batch`, `extract.specialist`, `markitdown.convert`
- **KEIN `brix run` via Bash** -> `mcp__brix__run_pipeline` oder `mcp__brix__pipeline__<name>`
- **KEIN Container-Rebuild** -> `pipelines/` und `helpers/` sind Volume-gemountet
- **IMMER `mcp__brix__get_tips` zuerst** bei Pipeline-Arbeit
- **IMMER `mcp__brix__list_bricks` / `search_bricks`** bevor ein Brick genutzt wird

Falls nicht registriert:
```bash
claude mcp add brix -- docker exec -i brix-mcp brix mcp
```

## CLI Referenz

```bash
brix --version
brix run <pipeline.yaml> -p key=value
brix validate <pipeline.yaml>
brix run --dry-run <pipeline.yaml>
brix server list        # registrierte MCP-Server
brix history            # letzte Runs
brix stats              # Erfolgsrate, Avg Duration
```

**NUTZE BRIX** fuer Multi-Step-Aufgaben statt einzelne Tool-Calls!

## Brick-Namespaces (Ueberblick)

| Namespace | Bricks |
|-----------|--------|
| `source.*` | `source.fetch` -- Daten holen via Connector |
| `db.*` | `db.query`, `db.upsert` -- Datenbankoperationen |
| `llm.*` | `llm.batch` -- LLM-Inferenz im Batch-Modus |
| `extract.*` | `extract.specialist` -- Deklarative Feldextraktion |
| `flow.*` | `filter`, `transform`, `aggregate`, `merge`, `dedup`, `diff`, `flatten`, `set`, `choose`, `switch`, `parallel`, `repeat`, `wait`, `validate`, `error_handler`, `pipeline`, `pipeline_group` |
| `action.*` | `action.notify`, `action.approval`, `action.respond`, `action.emit`, `action.queue` |
| `http.*` | `http.request` -- HTTP-Calls |
| `mcp.*` | `mcp.call` -- Beliebiger MCP-Server-Call |
| `script.*` | `script.python`, `script.cli` -- Code-Ausfuehrung |
| `markitdown.*` | `markitdown.convert` -- Dokumente zu Markdown |

Vollstaendige Liste: `mcp__brix__list_bricks()`

**Brick-Discovery:**
```python
mcp__brix__list_bricks()                    # alle Bricks
mcp__brix__search_bricks(query="email")     # nach Keyword
mcp__brix__get_brick_schema(name="llm.batch")  # Schema + Params
```

## Connectors

Connectors abstrahieren Authentifizierung und API-Details:

| Connector | Quelle |
|-----------|--------|
| `outlook` | M365 Outlook via M365 MCP |
| `gmail` | Gmail via IMAP |
| `onedrive` | OneDrive / SharePoint via M365 MCP |
| `paypal` | PayPal REST API |
| `sparkasse` | FinTS/HBCI |
| `local_files` | Lokales Dateisystem |

Vollstaendige Liste: `mcp__brix__list_connectors()`

## Wichtige MCP-Tools

```python
# Planung
mcp__brix__plan_pipeline(goal="...")          # Schrittweise Plan mit Brick-Namen
mcp__brix__compose_pipeline(goal="...")       # Fertige Pipeline-Definition

# Bricks
mcp__brix__list_bricks()                      # alle Bricks
mcp__brix__search_bricks(query="...")         # Suche
mcp__brix__get_brick_schema(name="...")       # Schema eines Bricks

# Connectors
mcp__brix__list_connectors()                  # alle Connectors
mcp__brix__get_connector(name="...")          # Details + Config
mcp__brix__connector_status(name="...")       # Verbindungstest

# Persistenter Store
mcp__brix__store_set(key="...", value="...")   # bleibt ueber Runs hinweg
mcp__brix__store_get(key="...")
mcp__brix__store_list()

# Secret Variables (Fernet-encrypted)
mcp__brix__set_variable(name="...", value="...", secret=True)
mcp__brix__get_variable(name="...")
mcp__brix__list_variables()

# Health
mcp__brix__server_health()                    # alle MCP-Server pruefen

# Backup / Restore
mcp__brix__run_pipeline(pipeline_id="system:backup")   # DB-Backup
mcp__brix__run_pipeline(pipeline_id="system:restore")  # DB-Restore

# Bundle Export/Import (project-level)
mcp__brix__bundle_export_project(project="...")  # Export ganzes Projekt als Bundle
mcp__brix__bundle_import_project(path="...")     # Import Projekt-Bundle

# Pin/Mock Testing
mcp__brix__test_pipeline(pipeline_id="...", pin={...}, mock={...})
```

## get_tips Topics

`mcp__brix__get_tips()` gibt eine Kurzreferenz zu:
- Brick-Namespaces
- Pipeline-Konventionen
- Haeufige Fehler

`mcp__brix__get_help(topic="<name>")` fuer tiefe Einblicke:

| Topic | Inhalt |
|-------|--------|
| `quick-start` | Erstes Pipeline-Setup in 5 Schritten |
| `step-referenzen` | Jinja2-Syntax: `{{ step_id.output }}`, `{{ item }}` |
| `foreach` | Parallel, batch_size, flatten, fetch_all_pages |
| `debugging` | diagnose_run, auto_fix_step, breakpoints |
| `error-patterns` | Haeufige Fehler + Fixes |
| `credentials` | Fernet-Encryption, UUID-Referenzen |
| `triggers` | schedule, event, file, http_poll, mail, pipeline_done |
| `dag` | depends_on, parallele Ausfuehrung |
| `templates` | get_template, instantiate_template |
| `helpers` | create_helper, register_helper |
| `registries` | registry_add, registry_search |
| `sdk` | Python SDK fuer Pipelines |
| `anti-patterns` | Was man NICHT tun soll |
| `tools` | Vollstaendige MCP-Tool-Referenz |
| `lessons-learned` | E2E-Erkenntnisse |

## Pfad-Konvention

Brix laeuft im Docker Container. Host-Dateisystem unter `/host/root/`:
- Host `/root/dev/...` -> Brix `/host/root/dev/...`
- Pipeline `output_dir`: `/host/root/pfad/zum/ziel`

## Source Architecture

```
src/brix/
  __init__.py          # Version + package init
  cli.py               # Click CLI entry point
  config.py            # Environment config (BRIX_DB_PATH, timeouts, etc.)
  db.py                # SQLite DB layer, connection management
  models.py            # Pydantic models for all entities
  engine.py            # Pipeline execution engine
  context.py           # Step execution context (Jinja2 rendering, step references)
  loader.py            # Pipeline/step loading from DB
  migrations.py        # Schema migration system (auto-run on start)
  migration_templates.py # Migration SQL/Python templates
  validator.py         # Pipeline validation
  bundle.py            # Project bundle export/import (tar.gz)
  integrity.py         # Referential integrity checks
  seed.py              # Built-in data seeding
  builtins.py          # Built-in brick/pipeline definitions

  bricks/              # Brick registry + schema + types
    registry.py        # Brick CRUD + lookup
    schema.py          # Brick schema validation
    builtins.py        # Built-in brick definitions
    types.py           # Brick type definitions

  runners/             # One runner per brick type (35 runners)
    base.py            # BaseRunner ABC
    filter.py          # flow.filter
    transform.py       # flow.transform
    llm_batch.py       # llm.batch
    specialist.py      # extract.specialist
    http.py            # http.request
    mcp.py             # mcp.call
    db_query.py        # db.query
    db_upsert.py       # db.upsert
    ...                # (aggregate, approval, choose, cli, dedup, diff,
                       #  emit, error_handler, flatten, markitdown, merge,
                       #  notify, parallel_runner, pipeline, pipeline_group,
                       #  python, queue, repeat, respond, set, source,
                       #  switch, validate, wait)

  mcp_handlers/        # MCP tool handler modules (one per entity domain)
    pipelines.py       # Pipeline CRUD + run + search
    steps.py           # Step CRUD
    helpers.py         # Helper CRUD + search
    bricks.py          # Brick listing + schema
    runs.py            # Run status, history, errors, logs
    triggers.py        # Trigger CRUD
    alerts.py          # Alert CRUD
    credentials.py     # Credential store
    connectors.py      # Connector status + config
    connections.py     # Connection management
    state.py           # Persistent store
    variables.py       # Secret variables
    registry.py        # Universal registry
    org.py             # Project/tag/group definitions
    templates.py       # Pipeline templates
    profiles.py        # Profiles/mixins
    health.py          # Health check
    backup.py          # DB backup/restore
    servers.py         # MCP server management
    testing.py         # Pin/mock testing
    insights.py        # Analytics/insights
    composer.py        # Pipeline composition (AI-assisted)
    discover.py        # Universal discovery
    help.py            # Help topics

  triggers/            # Trigger system
    service.py         # Trigger service (poll loop)
    runners.py         # Trigger type runners
    models.py          # Trigger models
    store.py           # Trigger state persistence
    state.py           # Trigger state management
    debounce.py        # Debounce logic

  templates/           # Built-in pipeline templates

  mcp_server.py        # MCP server (stdio + SSE)
  mcp_pool.py          # MCP connection pooling
  mcp_utils.py         # MCP utilities
  api.py               # REST API (Starlette)
  sdk.py               # Python SDK for pipelines
  scheduler.py         # Cron-based scheduler
  system_pipelines.py  # System pipelines (_system/)
  pipeline_store.py    # Pipeline storage layer
  helper_registry.py   # Helper registration
  credential_store.py  # Fernet-encrypted credential store
  server_manager.py    # MCP server lifecycle
  registry.py          # Universal registry
  profiles.py          # Profiles/mixins system
  connections.py       # Connection management
  connectors.py        # Connector implementations
  resilience.py        # Circuit breaker, rate limiter, saga
  cache.py             # Step-level caching
  security.py          # PII scan, security checks
  alerting.py          # Alert system
  history.py           # Run history
  progress.py          # Progress tracking
  debug_tools.py       # Debugging utilities
  testing.py           # Test utilities
  viz.py               # Pipeline visualization
  deps.py              # Dependency resolution
  export_seed_data.py  # Seed data export utility
```

## Key Features

- **DB-First**: Pipelines, Helpers, Bricks, Connectors, Tools, Help -- alles in brix.db
- **15 Entity-Typen** mit vollstaendigem CRUD (create/update/get/list/search/delete)
- **Org-Felder** (`project`/`tags`/`group`/`description`) auf allen Entities
- **org_registry**: Zentrale Definitions-Datenbank fuer Projekte, Tags und Groups
- **Auto-Tagging + Auto-Version-Bump**: Automatisch bei jedem Save
- **SSE Transport**: MCP-Server unterstuetzt SSE zusaetzlich zu stdio
- **unwrap_json**: Automatisches Entpacken verschachtelter JSON-Responses
- **PII-Scan Integration**: Gatekeeper prueft automatisch auf personenbezogene Daten
- **Resilience**: Circuit Breaker, Rate Limiter, Step-Level Cache, Saga
- **Advanced Flow**: Queue, Event Bus, Debounce, Streaming
- **Profiles/Mixins + Dynamic Dispatch + Brick-Vererbung**: Config-Overrides pro Environment
- **Pin/Mock Testing**: n8n-style Data Pinning fuer deterministische Offline-Tests
- **Secret Variables**: Fernet-verschluesselt, nur zur Laufzeit entschluesselt
- **Bundle Export/Import**: Projekt-Level Export/Import als tar.gz (`bundle_export_project` / `bundle_import_project`)
- **Health-Check** (`brix__health`): System-Gesundheitsbericht inkl. DB, Migrationen, Brick-Registry
- **Backup/Restore**: MCP-Tools fuer vollstaendiges DB-Backup und Restore
- **System-Pipelines** (`_system/`): Laufen automatisch bei Container-Start
- **Schema-Migration-System**: Automatisch beim Start, versioniert und idempotent
- **Universal Registry** (`discover()`): Bricks, Connectors, Helpers, Tools -- ein Einstiegspunkt
- **Custom Bricks + Referenz-Integritaet**: Eigene Bricks registrieren, Loeschutz bei Nutzung

## Neue Pipelines/Helpers erstellen

**KEIN Container-Rebuild noetig!** `pipelines/` und `helpers/` sind Volume-gemountet.

**Brick-First: IMMER erst pruefen ob ein Built-in Brick ausreicht.**

Nur wenn kein passender Brick existiert: `script.python` mit eigenem Helper.

**Helper-Boilerplate:**
```python
#!/usr/bin/env python3
"""Beschreibung."""
import json, sys

def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}
    # ... Logik ...
    print(json.dumps(result))

if __name__ == "__main__":
    main()
```

**Pipeline-Regeln:**
- `concurrency` = int (KEIN Jinja2-Template!)
- Host-Pfade: `/host/root/...`
- `| default([])` bei conditional Steps
- Brick-Namen: `namespace.type` (z.B. `flow.filter`, nicht `filter`)
- Erst `brix validate`, dann `brix run --dry-run`, dann `brix run`

## Entwicklung

### Test-Isolation mit BRIX_DB_PATH

Fuer Tests wird eine separate DB verwendet, damit `brix.db` nicht veraendert wird:

```bash
# Gezielte Tests (bevorzugt -- Host schonen)
PYTHONPATH=src python3 -m pytest tests/test_foo.py -v

# Alle Tests
PYTHONPATH=src python3 -m pytest tests/ -v

# Mit eigener DB fuer manuelle Tests
BRIX_DB_PATH=/tmp/test-brix.db brix run ...
```

### Rebuild

```bash
# Rebuild NUR bei src/brix/ oder Dockerfile Aenderungen
docker compose build --quiet && docker compose up -d

# KEIN Rebuild bei pipelines/ oder helpers/ Aenderungen!
```

### Deployment auf Collir

`sync-collir.sh` synchronisiert Code auf den Collir-Server:
- Pre-flight: Vergleicht Versionen und Migrationen
- Warnt bei destruktiven Migrationen (DROP/DELETE/RENAME)
- Erstellt Backup (DB + Projekt-Bundles) vor dem Sync
- rsync + rebuild + verify

```bash
./sync-collir.sh
```

## Cody-Projekt

- Slug: `forge`
- MCP-Transport: stdio + SSE
- DB-First: alles in brix.db

## Docs

- `docs/decisions.md` -- Architektur-Entscheidungen
- `docs/integration-learnings.md` -- Learnings aus E2E-Testing
- `docs/cookbook.md` -- Use-Case-Beispiele mit Brick-Pipelines
- `docs/runner-output-contracts.md` -- Output-Contracts fuer alle Runners
- `docs/schema-pk-reference.md` -- Schema Primary-Key Referenz
- `docs/review-*.md` -- Experten-Reviews
- `docs/research-*.md` -- MCP SDK + Skills Recherche
