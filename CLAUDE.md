# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Was ist Brix?

**Brix** ist eine Skill-Runtime und Prozess-Orchestrator für Claude Code. Ein CLI-Tool das Multi-Step-Workflows als Pipelines ausführt — MCP-Calls, HTTP-Requests, Shell-Commands und Python-Scripts in einem einzigen `brix run` Aufruf.

**Warum:** Jeder Tool-Call in Claude Code kostet Kontext-Tokens. 10 Tool-Calls = 10x Kontext. `brix run` macht daraus 1 Call. Token-Einsparung: ~99%.

## Brix ist verfügbar!

```bash
# CLI ist im PATH (Wrapper für docker exec brix brix)
brix --version          # 0.6.4
brix run <pipeline.yaml> -p key=value
brix validate <pipeline.yaml>
brix run --dry-run <pipeline.yaml>
brix server list        # registrierte MCP-Server
brix history            # letzte Runs
brix stats              # Erfolgsrate, Avg Duration
```

**NUTZE BRIX** für Multi-Step-Aufgaben statt einzelne Tool-Calls! Besonders für:
- MCP-Tool-Ketten (M365, Docker, n8n)
- Batch-Downloads mit paralleler Verarbeitung
- foreach-Operationen über Listen
- Alles was >3 sequentielle Tool-Calls wäre

## Pfad-Konvention

Brix läuft im Docker Container. Host-Dateisystem unter `/host/root/`:
- Host `/root/dev/...` → Brix `/host/root/dev/...`
- Pipeline output_dir: `/host/root/pfad/zum/ziel`

## Verfügbare Pipelines

```bash
brix list pipelines     # zeigt alle in /app/pipelines/
```

- **download-attachments** — M365 Mail-Attachments herunterladen (PDF-Filter, parallel)

## Skills (Slash-Commands)

- `/download-attachments` — M365 Attachments via Brix Pipeline
- `/brix-run` — Beliebige Brix Pipeline ausführen

## Architektur

- **5 Runner:** python (subprocess), http (httpx), cli (subprocess), mcp (stdio SDK), pipeline (Sub-Pipelines)
- **Pipeline-Format:** YAML + Jinja2 (SandboxedEnvironment)
- **Docker Container** mit Host-Integration (Docker Socket, Binary, Dateisystem)
- **MCP-Server** via `docker exec -i` (stdio transparent)

## Neue Pipelines/Helpers erstellen

**KEIN Container-Rebuild nötig!** `pipelines/` und `helpers/` sind Volume-gemountet.

**Helper-Boilerplate (IMMER dieses Pattern!):**
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
- Epic: E-BRIX-CORE (26 Tasks + Bugfixes)
- Version: 0.6.4

## Docs

- `docs/decisions.md` — 21 Architektur-Entscheidungen
- `docs/integration-learnings.md` — 8 Learnings aus E2E-Testing
- `docs/review-*.md` — Experten-Reviews
- `docs/research-*.md` — MCP SDK + Skills Recherche
