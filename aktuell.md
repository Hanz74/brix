# Brix — Aktueller Stand

**Datum:** 2026-03-22
**Version:** v3.4.0
**Status:** v1 + v2 + v3 komplett implementiert, alle E2E validiert

---

## Metriken

- **Version:** 3.4.0
- **Tests:** 667 (alle grün)
- **Tasks:** 63 total (27 v1 + 18 v2 + 18 v3), 0 offen
- **MCP Tools:** 14 + dynamische pipeline__* Tools (inkl. get_run_status, update_step)
- **Built-in Bricks:** 10+ (inkl. filter, transform)
- **Trigger:** 5 (MCP stdio, MCP HTTP, REST API, Webhook, Cron)
- **Container:** 3 (brix CLI, brix-mcp, brix-api)
- **Experten-Reviews:** 7

---

## E2E Ergebnis (gleicher Use Case, alle Methoden)

| Methode | Dauer | Tool-Calls | Token (geschätzt) | PDFs |
|---------|-------|-----------|-------------------|------|
| Ohne Brix (Claude einzeln) | ~10 Min+ | ~164 | ~656.000 | fragil |
| v1 CLI (brix run via Bash) | ~35s | 1 | ~5.000 | 50 |
| v2 MCP (nativ) | 35.4s | 1 | ~3.000 | 50 |

v3 Scaling: Auto-Pagination mit `fetch_all_pages` validiert gegen 33.000+ Items. Item-Level Checkpoints ermöglichen Resume ohne Re-Fetch.

---

## Container laufen mit v3.4.0

```
brix       — CLI mode (sleep infinity, docker exec)
brix-mcp   — MCP Server (stdio, Port 8091)
brix-api   — REST API (Port 8090)
```

---

## Epics

### E-BRIX-CORE (v1) — 27/27 Tasks ✅
Fundament, Runner, Engine, MCP, CLI, Testing, E2E

### E-BRIX-V2 (v2) — 18/18 Tasks ✅
Schema, MCP Server, Pipeline Store, REST API, Triggers, Templates, Security, E2E

### E-BRIX-V3 (v3) — 18/18 Tasks ✅
Auto-Pagination, Item-Level Resume, Rate-Limit Handling, batch_size, Flat foreach Output,
else_of Conditional Steps, Live Run Status, Heartbeat Detection, Async Dispatch, update_step MCP Tool

---

## v3 Features (alle 18 Tasks abgeschlossen)

| Feature | Status |
|---------|--------|
| Auto-Pagination (`fetch_all_pages`) | ✅ |
| Item-Level Resume (foreach Checkpoints) | ✅ |
| Rate-Limit Handling (429 + Retry-After) | ✅ |
| `batch_size` als foreach-Primitive | ✅ |
| Flat foreach Output (`flatten: true`) | ✅ |
| `else_of` Conditional Steps | ✅ |
| Live Run Status | ✅ |
| Heartbeat Detection | ✅ |
| Async Dispatch (`--async`, `run_id`) | ✅ |
| `update_step` MCP Tool | ✅ |
| `filter` Built-in Brick | ✅ |
| `transform` Built-in Brick | ✅ |

---

## Offene Tasks

Keine. 0 offene Tasks.

---

## Nächster Schritt

HA-Pattern-Analyse läuft — Evaluation von High-Availability Patterns für Brix (Multi-Worker, Failover, Persistent Queue).
