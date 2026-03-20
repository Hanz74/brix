# Brix — Aktueller Stand

**Datum:** 2026-03-19
**Version:** 0.6.4
**Status:** Implementation komplett, E2E validiert

---

## Was wurde gemacht (diese Session)

### Implementation: 6 Waves, 27 Tasks, 274 Tests
- Wave 1 (v0.1.x): Docker, Struktur, Models, Loader, Engine, CLI Runner, Click CLI
- Wave 2 (v0.2.x): Python Runner, HTTP Runner, foreach+parallel, Retry
- Wave 3 (v0.3.x): MCP Runner, Server Management, Schema Caching
- Wave 4 (v0.4.x): Sub-Pipelines, Workdir+Resume, Dry-Run, Conditions, Output-Feld
- Wave 5 (v0.5.x): SQLite History, Testing Framework, Validation, Discovery
- Wave 6 (v0.6.x): download-attachments Pipeline, Skill Migration, Cleanup

### E2E Validierung
- 50 Rechnungs-PDFs aus M365 Outlook heruntergeladen
- 6.9 MB, 78 Sekunden, 1 Tool-Call (statt ~164)
- Token-Einsparung: ~99%

### Bugs gefunden und gefixt (v0.6.3-0.6.4)
- Jinja2 Dict-Rendering: repr statt JSON (betraf ALLE Pipelines)
- Große Payloads: stdin statt argv bei >100KB
- PDF-Filter: nur .pdf Attachments durchlassen
- Parallel Attachments: concurrency 5 für Speedup

### 5 Experten-Reviews
1. Alex (Spec-Wächter): 3 CRITICAL, 7 HIGH → alle adressiert
2. Prof. Bruckner (DevOps): Docker, Hatchling, Click, SQLite
3. MCP SDK Recherche: ClientSessionGroup, Error-Ebenen
4. Skills Recherche: Frontmatter, $ARGUMENTS, allowed-tools
5. Alex (Strategy): Kein Loop/Until in Engine, Claude entscheidet Strategie

### 21 Architektur-Entscheidungen + 8 Integration Learnings

---

## Repo-Struktur

```
brix/
├── CLAUDE.md                      # Claude-Instruktionen (aktuell!)
├── README.md                      # Öffentliche Doku (englisch)
├── Dockerfile                     # Python 3.12-slim + uv
├── docker-compose.yml             # Docker Socket + Host-Mount
├── .env.example
├── pyproject.toml                 # v0.6.4, Hatchling
├── CHANGELOG.md
├── .claude/commands/              # Skills (projektspezifisch)
│   ├── download-attachments.md
│   └── brix-run.md
├── src/brix/
│   ├── cli.py                     # Click CLI (run, validate, server, history, stats, test, clean)
│   ├── engine.py                  # Pipeline Engine (foreach, parallel, retry, resume)
│   ├── loader.py                  # YAML + Jinja2 SandboxedEnvironment
│   ├── context.py                 # PipelineContext + Workdir
│   ├── models.py                  # 15 Pydantic Models
│   ├── history.py                 # SQLite Run History
│   ├── cache.py                   # MCP Schema Cache
│   ├── registry.py                # Pipeline/Brick Discovery
│   ├── validator.py               # Pipeline Validation
│   ├── testing.py                 # Mock-Fixture Testing
│   └── runners/
│       ├── base.py, cli.py, python.py, http.py, mcp.py, pipeline.py
├── pipelines/
│   └── download-attachments.yaml  # Erste vollständige Pipeline
├── helpers/
│   ├── extract_attachment_urls.py
│   ├── flatten_attachments.py
│   ├── save_attachment.py
│   └── summary_report.py
├── tests/                         # 274 Tests
│   ├── test_models.py (56), test_loader.py (27), test_engine.py (25+)
│   ├── test_runners.py (15), test_http_runner.py (16), test_mcp_runner.py (10)
│   ├── test_cli.py (22), test_context.py (11), test_workdir.py (8)
│   ├── test_cache.py (17), test_validator.py (15), test_registry.py (16)
│   ├── test_testing.py (9), test_helpers.py (6), test_pipeline_runner.py (4)
│   └── test_history.py (8)
└── docs/
    ├── decisions.md               # 21 Architektur-Entscheidungen
    ├── integration-learnings.md   # 8 Learnings aus E2E
    ├── review-spec-alex.md        # Spec-Review
    ├── review-packaging-bruckner.md
    ├── review-strategy-alex.md    # Strategy-Review (NEU)
    ├── research-mcp-sdk.md
    └── research-claude-code-skills.md
```

## Nächste Schritte

1. **Skill-Prompt erweitern:** Zwei Pipeline-Varianten (targeted vs broad), Claude wählt
2. **Pipeline-Strategien:** Claude orchestriert Pagination, nicht Brix-Engine
3. **Weitere Pipelines:** Neue Use Cases identifizieren und als Pipelines umsetzen
4. **Filter-Typ:** Revisit in v2 wenn visuelles Pipeline-UI kommt
