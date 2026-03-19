# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Was ist Brix?

**Brix** — Generischer Prozess-Orchestrator für Claude Code Skills. Kombiniert modulare Bausteine (Python, REST API, CLI, MCP Tools, DB, Docker) zu Pipelines mit einheitlichem JSON-Interface.

## Vision

Ein leichtgewichtiges Framework wo:
- **Bausteine** (Brix) verschiedenster Art ein einheitliches Interface haben: JSON in → JSON out
- **Pipelines** Bausteine verketten: Output von A → Input von B
- **Skills** (Claude Code Slash-Commands) die Pipelines orchestrieren
- **Parallelisierung** wo möglich, sequentiell wo nötig
- **Error-Handling** + Retry + Reporting eingebaut

## Baustein-Typen (geplant)

| Typ | Beschreibung | Beispiel |
|-----|-------------|---------|
| `python` | Python-Funktion oder Script | `brix.py:extract_data()` |
| `rest` | HTTP API Call | `POST https://api.example.com/v1/convert` |
| `cli` | Shell/Bash Command | `ffmpeg -i input.mp4 output.wav` |
| `mcp` | MCP Tool Call | `mcp__m365__list-mail-messages` |
| `db` | Datenbank-Query | `SELECT * FROM documents WHERE ...` |
| `docker` | Container ausführen | `docker exec markitdown-mcp ...` |

## Einheitliches Interface

Jeder Baustein:
- **Input**: JSON (oder stdin)
- **Output**: JSON (oder stdout)
- **Error**: Strukturiert `{"success": false, "error": "...", "step": "..."}`
- **Config**: ENV oder Parameter

## Erster Use Case

`/download-attachments` Skill — der den Anstoß für Brix gab:
1. `m365_fetch` Brix → Mails suchen, Attachments identifizieren
2. `http_download` Brix → Dateien parallel runterladen (Graph API)
3. `file_save` Brix → Mit strukturierten Dateinamen speichern
4. `markitdown_convert` Brix → Optional durch MarkItDown jagen
5. `report` Brix → Ergebnis-Report generieren

## Recherche-Ergebnis

Kein existierendes Tool füllt diese Lücke (Stand März 2026):
- **pypyr** (1.7k Stars) — am nächsten, aber kein JSON-Vertrag, kein REST/MCP
- **Conductor** (11k Stars) — zu heavyweight (Server-basiert)
- **Airflow/Prefect/Dagster** — Enterprise Data Pipelines, Overkill
- Lücke: Multiple Step-Typen + striktes JSON in/out + leichtgewichtig + Claude Code integrierbar

## Cody-Projekt

- Slug: `forge` (historisch, Name ist Brix)
- Phase: `ideation`

## Tech Stack (geplant)

- Python 3.11+
- asyncio + httpx (parallel HTTP)
- Click oder Typer (CLI)
- Pydantic (JSON-Schemas)
- Docker Container (optional)
