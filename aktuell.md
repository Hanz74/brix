# Brix — Aktueller Stand

**Datum:** 2026-04-06
**Version:** v7.86.5
**Baseline-Tag:** v7.77.2 (pre-DB-only)

## Abgeschlossene Epics (30.03–06.04)
- E-BRIX-STANDARDS (6) — Timeouts, validate_config, MCP-Format, Error-Handling
- DBQUAL (5) — Bundle export/import, Test-Isolation, pipeline_helpers
- E-BRIX-BUGFIX (6) — Engine-Fixes, config/params, Brick-Schemas
- E-BRIX-QUALITY (7) — CLAUDE.md, Tips-DB, Integrity-Sync, Singular Tables, Logging, Compat-Views
- E-BRIX-BRICKS (3+4) — 13+3 neue Bricks (file.*, flow.*, extract.*, util.*, db.exec, b64encode)
- E-BRIX-DBFIRST (4) — Seeds einmalig, builtins.py deprecated, MCP-Tools für alle Entities
- E-BRIX-SCHEDULER (3) — schedule Trigger-Typ, schedules.yaml → DB, Doku
- E-BRIX-TRIGFIX (3) — Boundary-Check, Timezone, last_fired_at, Crash-Recovery
- E-BRIX-DBONLY (15 done) — Pipeline-Persistenz YAML→DB-Rows, yaml_content entfernt

## Laufend: E-BRIX-DBONLY (DBO-16..21)
| Wave | Task | Was | Status |
|------|------|-----|--------|
| 2 | DBO-16 | Brick-Registry Lücken | Codex running |
| 2 | DBO-17 | Sub-Pipeline Runner DB-first | pending |
| 3 | DBO-18 | yaml_content Refs entfernen | pending |
| 4 | DBO-19 | Help-Topics + Multi-Group Doku | pending |
| 4 | DBO-20 | Tool-Schema Platzhalter | pending |
| 5 | DBO-21 | Integrity-Check alle Entity-Typen | pending |

## Offene Inbox
- INBOX-429: Lessons-Learned Help-Topic
- INBOX-432: Cookbook
- INBOX-511: Pause/Resume
- INBOX-544: Brix-UI Schema-Änderungen prüfen

## Architektur
- DB-only: pipeline_step, pipeline_credential, pipeline_input
- yaml_content tot (Spalte bleibt, Code liest/schreibt nicht mehr)
- config→params Merge im Lade-Pfad
- Multi-Group über tags group: Prefix
- Schedule Trigger: Boundary-Check + Timezone + Crash-Recovery
- Codex als Implementierungs-Agent, Opus als Orchestrator
