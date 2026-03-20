# Brix — Aktueller Stand

**Datum:** 2026-03-20
**Version:** v2.6.2
**Status:** v2 komplett implementiert, MCP E2E validiert

---

## v2 — FERTIG und E2E validiert

### Metriken
- **Version:** 2.6.2
- **Tests:** 589 (alle grün)
- **Tasks:** 18/18 (E-BRIX-V2) + 27 (E-BRIX-CORE) + 5 Bugs = 50 total
- **MCP Tools:** 14 + dynamische pipeline__* Tools
- **Built-in Bricks:** 10
- **Trigger:** 5 (MCP stdio, MCP HTTP, REST API, Webhook, Cron)
- **Container:** 3 (brix CLI, brix-mcp, brix-api)
- **Experten-Reviews:** 7

### E2E Ergebnis (gleicher Use Case, alle Methoden)

| Methode | Dauer | Tool-Calls | Token (geschätzt) | PDFs |
|---------|-------|-----------|-------------------|------|
| Ohne Brix (Claude einzeln) | ~10 Min+ | ~164 | ~656.000 | fragil |
| v1 CLI (brix run via Bash) | ~35s | 1 | ~5.000 | 50 |
| **v2 MCP (nativ)** | **35.4s** | **1** | **~3.000** | **50** |

### Container laufen
```
brix       — CLI mode (sleep infinity, docker exec)
brix-mcp   — MCP Server (stdio, Port 8091)
brix-api   — REST API (Port 8090)
```

### MCP Registration
```bash
claude mcp add brix -- docker exec -i brix-mcp brix mcp
```

---

## Offene Bugs/Improvements (5 Tasks)

| Task | Typ | Priorität | Problem |
|------|-----|-----------|---------|
| T-BRIX-V2-19 | bugfix | high | Pipeline-Pfad Inkonsistenz (app/ vs brix/) |
| T-BRIX-V2-20 | bugfix | medium | Stille Ignorierung unbekannter Parameter |
| T-BRIX-V2-21 | bugfix | medium | Datei-Duplikate werden überschrieben |
| T-BRIX-V2-22 | bugfix | medium | skip/offset fehlt in broad Pipeline |
| T-BRIX-V2-23 | doku | high | claude mcp add Setup-Anleitung |

---

## Epics

### E-BRIX-CORE (v1) — 27/27 Tasks ✅
Fundament, Runner, Engine, MCP, CLI, Testing, E2E

### E-BRIX-V2 (v2) — 18/18 Tasks ✅ + 5 Bugs offen
Schema, MCP Server, Pipeline Store, REST API, Triggers, Templates, Security, E2E

---

## Nächster Schritt
5 Bugs/Improvements fixen (V2-19 bis V2-23)
