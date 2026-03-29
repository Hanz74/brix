# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Was ist Brix?

**Brix** ist ein DB-First Pipeline-Orchestrator für Claude Code. Alles lebt in `brix.db` — Pipelines, Helpers, Bricks, Connectors, Tools, Help Topics. Workflows werden aus 59 Bricks zusammengesteckt — konfigurieren statt coden. 81 Pipelines, 30 Helpers, ~3800+ Tests. MCP-Server mit stdio + SSE Transport.

**Warum:** Jeder Tool-Call in Claude Code kostet Kontext-Tokens. `brix run` macht 164 Calls zu einem. Token-Einsparung: ~99%.

## MCP Server (empfohlen)

Brix ist als MCP Server registriert. Claude sieht `mcp__brix__*` Tools automatisch.

### WICHTIG: Brick-First Regeln

- **KEIN YAML manuell schreiben** → `mcp__brix__create_pipeline` oder `mcp__brix__compose_pipeline`
- **KEINE Helper-Scripts für Standardaufgaben** → Built-in Bricks: `flow.filter`, `flow.transform`, `llm.batch`, `extract.specialist`, `markitdown.convert`
- **KEIN `brix run` via Bash** → `mcp__brix__run_pipeline` oder `mcp__brix__pipeline__<name>`
- **KEIN Container-Rebuild** → `pipelines/` und `helpers/` sind Volume-gemountet
- **IMMER `mcp__brix__get_tips` zuerst** bei Pipeline-Arbeit
- **IMMER `mcp__brix__list_bricks` / `search_bricks`** bevor ein Brick genutzt wird

Falls nicht registriert:
```bash
claude mcp add brix -- docker exec -i brix-mcp brix mcp
```

## Brix ist verfügbar!

```bash
brix --version          # 7.52.0
brix run <pipeline.yaml> -p key=value
brix validate <pipeline.yaml>
brix run --dry-run <pipeline.yaml>
brix server list        # registrierte MCP-Server
brix history            # letzte Runs
brix stats              # Erfolgsrate, Avg Duration
```

**NUTZE BRIX** für Multi-Step-Aufgaben statt einzelne Tool-Calls!

## Brick-Namespaces (Überblick)

| Namespace | Bricks |
|-----------|--------|
| `source.*` | `source.fetch` — Daten holen via Connector |
| `db.*` | `db.query`, `db.upsert` — Datenbankoperationen |
| `llm.*` | `llm.batch` — LLM-Inferenz im Batch-Modus |
| `extract.*` | `extract.specialist` — Deklarative Feldextraktion |
| `flow.*` | `filter`, `transform`, `aggregate`, `merge`, `dedup`, `diff`, `flatten`, `set`, `choose`, `switch`, `parallel`, `repeat`, `wait`, `validate`, `error_handler`, `pipeline`, `pipeline_group` |
| `action.*` | `action.notify`, `action.approval`, `action.respond`, `action.emit`, `action.queue` |
| `http.*` | `http.request` — HTTP-Calls |
| `mcp.*` | `mcp.call` — Beliebiger MCP-Server-Call |
| `script.*` | `script.python`, `script.cli` — Code-Ausführung |
| `markitdown.*` | `markitdown.convert` — Dokumente zu Markdown |

59 Bricks gesamt — `mcp__brix__list_bricks()` für die vollständige Liste.

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

## Wichtige MCP-Tools

```python
# Planung
mcp__brix__plan_pipeline(goal="...")          # Schrittweise Plan mit Brick-Namen
mcp__brix__compose_pipeline(goal="...")       # Fertige Pipeline-Definition

# Bricks (59 total, 10 Namespaces)
mcp__brix__list_bricks()                      # alle Bricks
mcp__brix__search_bricks(query="...")         # Suche
mcp__brix__get_brick_schema(name="...")       # Schema eines Bricks

# Connectors (6 total)
mcp__brix__list_connectors()                  # alle Connectors
mcp__brix__get_connector(name="...")          # Details + Config
mcp__brix__connector_status(name="...")       # Verbindungstest

# Persistenter Store
mcp__brix__store_set(key="...", value="...")   # bleibt über Runs hinweg
mcp__brix__store_get(key="...")
mcp__brix__store_list()

# Secret Variables (Fernet-encrypted)
mcp__brix__set_variable(name="...", value="...", secret=True)
mcp__brix__get_variable(name="...")
mcp__brix__list_variables()

# Health
mcp__brix__server_health()                    # alle MCP-Server prüfen

# Backup / Restore
mcp__brix__run_pipeline(pipeline_id="system:backup")   # DB-Backup
mcp__brix__run_pipeline(pipeline_id="system:restore")  # DB-Restore

# Pin/Mock Testing
mcp__brix__test_pipeline(pipeline_id="...", pin={...}, mock={...})
```

## get_tips Topics

`mcp__brix__get_tips()` gibt eine Kurzreferenz zu:
- Brick-Namespaces
- Pipeline-Konventionen
- Häufige Fehler

`mcp__brix__get_help(topic="<name>")` für tiefe Einblicke:

| Topic | Inhalt |
|-------|--------|
| `quick-start` | Erstes Pipeline-Setup in 5 Schritten |
| `step-referenzen` | Jinja2-Syntax: `{{ step_id.output }}`, `{{ item }}` |
| `foreach` | Parallel, batch_size, flatten, fetch_all_pages |
| `debugging` | diagnose_run, auto_fix_step, breakpoints |
| `error-patterns` | Häufige Fehler + Fixes |
| `credentials` | Fernet-Encryption, UUID-Referenzen |
| `triggers` | Mail, file, http_poll, pipeline, webhook |
| `dag` | depends_on, parallele Ausführung |
| `templates` | get_template, instantiate_template |
| `helpers` | create_helper, register_helper |
| `registries` | registry_add, registry_search |
| `sdk` | Python SDK für Pipelines |
| `anti-patterns` | Was man NICHT tun soll |
| `tools` | Vollständige MCP-Tool-Referenz |
| `lessons-learned` | E2E-Erkenntnisse |

## Pfad-Konvention

Brix läuft im Docker Container. Host-Dateisystem unter `/host/root/`:
- Host `/root/dev/...` → Brix `/host/root/dev/...`
- Pipeline `output_dir`: `/host/root/pfad/zum/ziel`

## v7.52.0 Features

- **DB-First**: Pipelines, Helpers, Bricks, Connectors, Tools, Help — alles in brix.db. Hard Cut: Code-Dateien entfernt.
- **15 Entity-Typen** mit vollständigem CRUD (create/update/get/list/search/delete)
- **Org-Felder** (`project`/`tags`/`group`/`description`) auf allen 15 Entities — Pflicht-Warnings bei fehlenden Feldern
- **org_registry**: Zentrale Definitions-Datenbank für Projekte, Tags und Groups
- **Auto-Tagging + Auto-Version-Bump**: Automatisch bei jedem Save
- **SSE Transport**: MCP-Server unterstützt SSE zusätzlich zu stdio (Cody-Bridge, Browser-Clients)
- **unwrap_json**: Automatisches Entpacken verschachtelter JSON-Responses in Downstream-Steps
- **PII-Scan Integration**: Gatekeeper prüft automatisch auf personenbezogene Daten (BLOCK-Severity)
- **Resilience**: Circuit Breaker, Rate Limiter, Step-Level Cache, Saga (kompensatorische Transaktionen)
- **Advanced Flow**: Queue, Event Bus, Debounce, Streaming
- **Profiles/Mixins + Dynamic Dispatch + Brick-Vererbung**: Config-Overrides pro Environment, geteilte Step-Sequenzen, Runtime-Routing
- **Pin/Mock Testing**: n8n-style Data Pinning für deterministische Offline-Tests
- **Secret Variables**: Fernet-verschlüsselt, nur zur Laufzeit entschlüsselt
- **Health-Check** (`brix__health`): System-Gesundheitsbericht inkl. DB, Migrationen, Brick-Registry
- **Backup/Restore**: MCP-Tools für vollständiges DB-Backup und Restore
- **System-Pipelines** (`_system/`): Laufen automatisch bei Container-Start
- **Schema-Migration-System**: Automatisch beim Start, versioniert und idempotent
- **Universal Registry** (`discover()`): Bricks, Connectors, Helpers, Tools — ein Einstiegspunkt
- **Custom Bricks + Referenz-Integrität**: Eigene Bricks registrieren, Löschschutz bei Nutzung

## Neue Pipelines/Helpers erstellen

**KEIN Container-Rebuild nötig!** `pipelines/` und `helpers/` sind Volume-gemountet.

**Brick-First: IMMER erst prüfen ob ein Built-in Brick ausreicht.**

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

## Entwicklung (nur bei src/ Änderungen)

```bash
# Tests
PYTHONPATH=src python3 -m pytest tests/ -v

# Rebuild NUR bei src/brix/ oder Dockerfile Änderungen
docker compose build --quiet && docker compose up -d

# KEIN Rebuild bei pipelines/ oder helpers/ Änderungen!
```

## Cody-Projekt

- Slug: `forge`
- Version: 7.52.0
- Tests: ~3800+
- Bricks: 59 (10 Namespaces)
- Helpers: 30
- Pipelines: 81 (buddy: 34, cody: 36, utility: 7, system: 4)
- MCP-Transport: stdio + SSE
- DB-First: alles in brix.db (Hard Cut — Code-Dateien entfernt)

## Docs

- `docs/decisions.md` — Architektur-Entscheidungen
- `docs/integration-learnings.md` — Learnings aus E2E-Testing
- `docs/cookbook.md` — Use-Case-Beispiele mit Brick-Pipelines
- `docs/review-*.md` — Experten-Reviews
- `docs/research-*.md` — MCP SDK + Skills Recherche
