# Brix — Aktueller Stand

**Datum:** 2026-03-20
**Version:** v1 = 0.6.5 (produktiv), v2 = geplant (2.0.0)

---

## v1 — FERTIG und produktiv

### Metriken
- **Version:** 0.6.5
- **Tests:** 277 (alle grün)
- **Tasks:** 27 erledigt (E-BRIX-CORE)
- **Pipelines:** 3 (download-attachments, download-attachments-broad, convert-folder)
- **Helpers:** 8 Python-Scripts
- **Integration Learnings:** 12 dokumentiert
- **Experten-Reviews:** 5 (Alex, Bruckner, MCP SDK, Skills, Strategy)

### E2E validiert
- 79 Rechnungs-PDFs aus M365 Outlook (17 MB, 45s, 3 Calls)
- 80 PDFs → Markdown konvertiert (63s, 1 Call)
- Token-Einsparung: ~99%

### Container läuft
```bash
brix --version          # 0.6.5
brix list pipelines     # 3 Pipelines
brix server list        # m365 registriert
```

---

## v2 — GEPLANT (Epic E-BRIX-V2)

### Vision
Brix wird vom CLI-Tool zum **MCP Server**. Jeder MCP-Client kann Brix nutzen (Claude Code, Claude Desktop, Cursor, n8n, Custom). Plus REST API für Cron/Webhooks.

### 18 Tasks, 7 Waves

```
Wave 1 (v2.0.x): Schema-System + Brick Registry + Filter/Transform
  T-BRIX-V2-01  Brick Schema System (Pydantic → JSON Schema)
  T-BRIX-V2-02  Brick Registry (Built-in + MCP Auto-Discovery)
  T-BRIX-V2-03  Filter + Transform Bricks

Wave 2 (v2.1.x): MCP Server + Core Tools
  T-BRIX-V2-04  MCP Server Grundgerüst (stdio)
  T-BRIX-V2-05  Discovery Tools (get_tips, list/search_bricks, get_schema)
  T-BRIX-V2-06  Builder Tools (create/get/add/remove/validate pipeline)
  T-BRIX-V2-07  Execution Tools (run, status, history + Dual-Layer Errors)

Wave 3 (v2.2.x): Pipeline Store + Auto-Exposure
  T-BRIX-V2-08  Pipeline Store (save/load/list/version)
  T-BRIX-V2-09  Pipeline Auto-Exposure als pipeline__<name> Tools

Wave 4 (v2.3.x): REST API + Triggers
  T-BRIX-V2-10  REST API (Run-Only HTTP Endpoint)
  T-BRIX-V2-11  Webhook + Cron Trigger

Wave 5 (v2.4.x): HTTP Transport + Connection
  T-BRIX-V2-12  MCP HTTP/SSE Transport
  T-BRIX-V2-13  Connection Pooling + Reconnect

Wave 6 (v2.5.x): Templates + Progress
  T-BRIX-V2-14  Pipeline Templates (get_template)
  T-BRIX-V2-15  Progress Streaming + Structured Logging

Wave 7 (v2.6.x): Security + E2E
  T-BRIX-V2-16  Security Hardening (Allowlists, shell=False)
  T-BRIX-V2-17  v1 Backward Compatibility
  T-BRIX-V2-18  E2E Test (MCP + REST + Cron)
```

### v2 Experten-Reviews (7 total)
1. Alex + Flowstein: Schema-System, n8n-Abgrenzung, Scope
2. Lisa (DX): Pipeline in 2 Calls, get_template, search_bricks, Dual-Layer Errors
3. Bruckner (DevOps): stdio first, Lazy Connection Pooling, Memory 512M
4. SecOps: shell=False, no inline eval, Allowlists, API-Key
5. Markt-Recherche: MCPStack, mcp-agent, n8n — keiner füllt die Lücke

### Killer-Features v2
- **pipeline__<name>**: Gespeicherte Pipelines werden automatisch MCP-Tools
- **MCP Auto-Discovery**: MCP-Server-Tools werden automatisch Bricks
- **REST API**: Pipelines via curl/cron/n8n/webhook auslösbar
- **get_tips**: Claude weiß nach Session-Start sofort alles über Brix

### Inbox: leer (alle 7 Items resolved/dismissed)

---

## Repo-Struktur

```
brix/
├── CLAUDE.md, README.md, Dockerfile, docker-compose.yml
├── pyproject.toml (v0.6.5), CHANGELOG.md
├── .claude/commands/ (download-attachments.md, brix-run.md)
├── src/brix/ (14 Module, 5 Runner)
├── pipelines/ (3 Pipelines)
├── helpers/ (8 Scripts)
├── tests/ (277 Tests)
└── docs/ (8 Reviews + Decisions + Learnings)
```

## Nächster Schritt

Wave 1 von E-BRIX-V2 starten: Schema-System + Brick Registry.
