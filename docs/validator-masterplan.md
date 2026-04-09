# Validator Masterplan

Status: 2026-04-08  
Scope: [src/brix/validator.py](/root/docker/brix/src/brix/validator.py), Validator-Callsites, Brick-/Runner-Schemas, DB-first/Brick-first alignment

## 1. Problem Summary

Der aktuelle Validator ist funktional gewachsen, aber nicht als stringente Architektur gebaut. Er erkennt bereits viele Fehlerbilder, ist aber:

- zu monolithisch
- zu stark auf heuristische String-Scans gestützt
- an mehreren Stellen noch implizit `dict`-zentriert
- uneinheitlich im Umgang mit Legacy-Typen vs. Brick-Typen
- noch kein echtes Advice-/Fix-System

Das Resultat ist vorhersagbar: Lowbrainer-Regressions wie `params=list` werden an einzelnen Stellen gefixt, aber an benachbarten Validator-Pfaden bricht es weiter.

## 2. Was heute schon gut ist

Der Validator kann bereits mehr als ein reiner Syntax-Checker:

- Referenz-Checks auf frühere Steps
- Jinja-AST-Checks inkl. Unknown Roots
- Brick-/Runner-Schema-Prüfung via JSON Schema
- Helper-/Connection-/Subpipeline-Checks
- Schema-Contracts zwischen Steps
- Extended Checks VAL-01 bis VAL-11
- Hinweise (`hint`) und `schema_ref` im Befundtext

Das ist eine gute Basis. Das Problem ist weniger “zu wenig Checks” als “zu wenig Systematik”.

## 3. Zentrale Erkenntnisse

### 3.1 Der Validator ist noch nicht konsequent shape-safe

`Step.params` ist inzwischen `dict | list | None`, aber große Teile von `validator.py` wurden historisch für `dict` gebaut. Zwar existieren Helper wie `_params_values()`, `_params_items()`, `_params_keys()`, `_params_get()`, aber die Nutzung ist nicht vollständig konsequent. Das ist exakt die Klasse von Fehlern, die wieder und wieder auftritt.

Konsequenz:

- Alle Validator-Pfade müssen gegen die tatsächlichen Modell-Shapes geschrieben werden.
- Der Validator darf niemals aus Datenform-Annahmen crashen.
- “Unprüfbar” muss zu Warning/Info führen, nie zu Exceptions.

### 3.2 Legacy- und Brick-Welt sind noch nicht sauber vereinheitlicht

Ein Teil der Checks arbeitet auf `step.type == "mcp"` oder `step.type == "pipeline"`, andere auf `effective_type = LEGACY_ALIASES.get(...)`. Das ist inkonsistent und bricht DB-first/Brick-first-Denken.

Ziel:

- Intern immer mit `effective_type` arbeiten.
- Legacy-Names nur noch als Input-Alias behandeln.
- Alle validationspezifischen Typregeln an Brick-Namen hängen.

### 3.3 Der Validator ist eine Sammlung von Checks, aber kein Analyse-System

Heute liefert `ValidationResult`:

- `errors`
- `warnings`
- `infos`
- `checks`

Das reicht für “meckern”, aber nicht für “erklären, priorisieren, reparieren”. Es fehlt:

- strukturierte Findings
- Severity
- Kategorie
- betroffene Felder
- konkrete Änderungsvorschläge
- optionale Auto-Fix-Hinweise

### 3.4 Es gibt doppelte Wissensquellen

Aktuell hängen Validierungsregeln verteilt in:

- `validator.py`
- Brick-Schemas
- Runner-`config_schema()`
- `runner-output-contracts.md`
- Composer-Type-Checks
- Engine-Merge-/Config-Logik

Das erzeugt Drift. Ein Super-Validator braucht eine definierte Truth-Order.

## 4. Zielbild: Super-Validator

Der Ziel-Validator soll 5 Dinge leisten:

1. Nie crashen, unabhängig von Input-Shape oder Legacy-Daten.
2. Brick-first validieren: Typ, Config, Input/Output, Referenzen, Contracts.
3. Findings strukturiert zurückgeben, nicht nur freie Strings.
4. Erklären, warum etwas problematisch ist.
5. Konkrete Vorschläge und später Auto-Fixes anbieten.

## 5. Zielarchitektur

## 5.1 Phase 1: Normalized Validation Context

Vor jedem Check wird eine einheitliche Read-Only-Analyseansicht gebaut:

- `effective_type`
- normalisierte Param-Sicht
- normalisierte Config-Sicht
- bekannte Referenzen
- statische/dynamische Werte
- bekannte Brick-/Runner-Schemas
- bekannte output/input types

Vorschlag:

- neue interne Struktur `ValidationContext`
- neue interne Struktur `StepAnalysis`

Damit arbeiten Checks nicht mehr direkt auf rohen `Step`-Objekten.

## 5.2 Phase 2: Structured Findings

`ValidationResult` sollte mittelfristig strukturierte Findings tragen:

```python
{
  "code": "T-BRIX-VAL-011",
  "severity": "warning",
  "step_id": "write",
  "field": "config.params",
  "message": "...",
  "why": "...",
  "hint": "...",
  "suggestion": {
    "kind": "rewrite",
    "target": "config.params",
    "example": ["{{ source.output.id }}"]
  },
  "schema_ref": "get_brick_schema(name='db.exec')"
}
```

Stringlisten können als Kompatibilitätsschicht daraus gerendert werden.

## 5.3 Phase 3: Check Families statt Monolith

Checks sollten in klaren Familien organisiert werden:

- shape safety
- type resolution
- schema validation
- reference resolution
- control-flow validation
- data-flow/type-flow validation
- persistence/DB-first checks
- helper/subpipeline checks
- lint/perf checks
- suggestion/autofix checks

## 5.4 Phase 4: Advice Engine

Jeder Check soll optional liefern:

- `why`
- `risk`
- `suggestion`
- `example_fix`
- `autofixable: bool`

Das macht den Validator endlich agentenfreundlich.

## 6. Was konkret noch validiert werden sollte

### 6.1 Shape Safety / Robustheit

- `params` als `list`, `dict`, `None`
- `config` als `dict`, `None`, malformed data from DB
- `choices`, `default_steps`, `sub_steps`, `sequence` auf korrekte Container-Shapes
- `depends_on` nur Liste von Strings
- `output`/`output_slots` nur mit erlaubten Werttypen
- niemals `.get()`, `.values()`, `.items()` ohne Shape-Guard

### 6.2 Brick-first Konsistenz

- unbekannter `effective_type`
- Brick vorhanden, aber Runner fehlt
- Brick-Schema und Runner-`config_schema()` widersprechen sich
- Legacy-Step-Typ trotz `strict_bricks`
- Top-level Felder vs. `config` vs. `params` widersprechen Brick-Semantik

### 6.3 DB-first Spezifisches

- Checks gegen DB-readback-Shape, nicht nur YAML-Shape
- Validierung für `load_from_db()`-Pipelines als First-Class-Fall
- Mismatch zwischen DB-merge semantics und Validator-Annahmen
- Warnung wenn Check nur für YAML stimmt, aber DB-Persistenz anders merged

### 6.4 Data Flow / Type Flow

- Step-Referenzen nicht nur per Regex, sondern semantisch nach Nutzungskontext
- `db.exec params` erwartet positional list
- `db.query params` erwartet named dict
- `flow.merge`, `foreach`, `db.upsert`, `flow.filter`, `flow.flatten` noch vollständiger typisieren
- `runner-output-contracts.md` oder Brick-Output-Type als Validator-Input nutzen

### 6.5 Control Flow

- `depends_on` auf unbekannte oder spätere Steps
- Konflikte aus `when`, `else_of`, `depends_on`
- potenziell unerreichbare Steps
- Steps mit Seiteneffekt in toten Branches
- `repeat`- und `choose`-Kinder sauber rekursiv validieren

### 6.6 Suggestions / Explainability

- “did you mean” bei Feld-/Step-/Brick-Namen
- konkrete Rewrite-Vorschläge
- Verweis auf Brick-Schema
- Verweis auf kompatiblen Brick
- Converter-Vorschlag bei Type-Mismatch
- Advice, ob es Warning oder echter Laufzeit-Fehler ist

## 7. Konkrete Schwächen im aktuellen Validator

- Monolithische `validate()`-Methode mit vielen ad-hoc Schleifen
- Checks arbeiten teils auf `step.type`, teils auf `effective_type`
- viele String-/Regex-Heuristiken ohne gemeinsame Referenzanalyse
- `ValidationResult` ist textzentriert statt strukturzentriert
- schwere Checks und leichte Checks sind nicht sauber getrennt
- Rekursion für nested structures ist nicht der dominierende Grundansatz
- es gibt keine zentrale “safe access layer” für `Step`

## 8. Priorisierte Umsetzung

### P0: Crash-Freiheit und Invarianten

- Validator auf vollständige Shape-Safety härten
- alle `params/config`-Pfadannahmen zentralisieren
- rekursive Checks für nested step containers absichern
- Tests für list/dict/null/malformed Varianten ausbauen

### P1: Brick-first Vereinheitlichung

- überall `effective_type`
- Legacy-Regeln zentralisieren
- Brick-/Runner-Schema-Auflösung als einzelne Normalisierungsschicht

### P2: Structured Findings

- interne Finding-Struktur einführen
- String-Output als Adapter beibehalten
- Codes, Severity, Hint, Suggestion standardisieren

### P3: Advice + Auto-Fix Hooks

- pro Check konkrete Suggestions
- optional `autofix_hint`
- langfristig Integration mit `auto_fix_step`/`update_step`

## 9. Cody-Style Task Plan

## T-VAL-001 Shape-Safe Access Layer

Goal:
Einführen einer zentralen Analyse-/Access-Schicht für `Step`, damit kein Check direkt rohe Shapes annimmt.

Deliverables:

- `ValidationContext`
- `StepAnalysis`
- Helper für normalized `params`, `config`, `effective_type`, references

Done when:

- kein Check greift mehr ungeguarded auf `params/config` zu
- neue Regressionstests für list/dict/None sind grün

## T-VAL-002 Validator Pass Refactor

Goal:
`validate()` in klar getrennte Passes zerlegen.

Deliverables:

- `run_core_checks()`
- `run_schema_checks()`
- `run_reference_checks()`
- `run_flow_checks()`
- `run_deep_checks()`

Done when:

- `validate()` nur noch orchestriert
- Checks sind nach Familien gruppiert

## T-VAL-003 Effective-Type Unification

Goal:
Alle Typregeln konsequent auf Brick-/`effective_type` umstellen.

Deliverables:

- keine `step.type == "mcp"`/`"pipeline"` Sonderlogik ohne Alias-Auflösung
- zentrale Typnormalisierung

Done when:

- Legacy und dot-notation liefern dieselben Validator-Ergebnisse

## T-VAL-004 Structured Findings

Goal:
`ValidationResult` von Stringlisten auf strukturierte Findings erweitern.

Deliverables:

- `ValidationFinding` Modell
- Adapter auf bisherige `errors/warnings/infos/checks`

Done when:

- MCP/CLI können weiterhin alte Strings lesen
- intern liegen strukturierte Findings vor

## T-VAL-005 Advice Engine

Goal:
Jeder wichtige Check liefert `why`, `hint`, `suggestion`, optional `example_fix`.

Deliverables:

- standardisierte Suggestion-Struktur
- mindestens für VAL-01, VAL-05, VAL-06, VAL-07, VAL-11

Done when:

- häufige Validator-Befunde enthalten konkrete nächste Aktion

## T-VAL-006 DB-first Validation Coverage

Goal:
Validator gegen DB-readback und Merge-Semantik härten.

Deliverables:

- Tests mit `PipelineStore.save()` + `load()` + `validate()`
- Tests für `config.params`/`params`/Top-level-Merge

Done when:

- YAML- und DB-geladene Pipelines verhalten sich validatorseitig gleich

## T-VAL-007 Nested Structure Validation

Goal:
`choose`, `repeat`, `parallel`, `pipeline_group` rekursiv und vollständig validieren.

Deliverables:

- rekursive Traversal Utility
- Checks auf Child-Steps statt nur top-level

Done when:

- nested invalid references/configs werden zuverlässig gefunden

## T-VAL-008 Type-Flow Expansion

Goal:
Output/Input-Kompatibilität über mehr Runnerfamilien abdecken.

Deliverables:

- formalisierte Typregeln für db/query/exec/upsert/filter/merge/flatten/foreach
- Konsolidierung mit `runner-output-contracts.md`

Done when:

- Type-Checks sind nicht mehr heuristisch punktuell, sondern systematisch

## T-VAL-009 Validator UX Surface

Goal:
CLI und MCP sollen Findings verständlich, priorisiert und agententauglich ausgeben.

Deliverables:

- Severity-Sortierung
- Summary-Zähler pro Kategorie
- konkrete “next actions”

Done when:

- `validate_pipeline` liefert mehr als nur freie Textlisten

## 10. Teststrategie

- unit tests pro Check-Familie
- shape-matrix tests für `params/config`
- DB-roundtrip tests
- nested-structure tests
- legacy-vs-brick parity tests
- snapshot-artige output tests für Findings

Pflicht-Matrix:

- YAML load
- DB load
- quick
- standard
- deep

## 11. Empfehlung für den ersten echten Sprint

Nicht sofort Advice Engine bauen. Erst:

1. T-VAL-001
2. T-VAL-003
3. T-VAL-006
4. T-VAL-007

Wenn diese vier sitzen, verschwinden die meisten peinlichen Lowbrainer-Bugs. Erst danach lohnt sich T-VAL-004/005 für Super-Validator-UX.

## 12. Harte Regel für die Zukunft

Jede Änderung an `Step`, Brick-Schema, DB-Merge oder Runner-Config braucht:

- mindestens einen Validator-Test
- mindestens einen DB-roundtrip-Test
- mindestens einen Legacy-vs-dot-notation-Test, falls betroffen

Sonst reproduzierst du genau das Muster, das gerade nervt: lokaler Fix, benachbarter Validator-Pfad kaputt.
