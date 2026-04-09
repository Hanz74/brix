# Brix Anti-Workaround Strategy

## Problem Statement

Brix-Buddy zeigt gerade ein systemisches Muster: echte Produktbugs werden nicht lokal, präzise und handlungsleitend genug sichtbar. Dadurch bauen LLMs Workarounds in produktiven Pipelines statt den Produktfehler sauber zu isolieren. Das kostet Zeit, verschlechtert die Architektur und senkt das Vertrauen in Brix.

Das Problem ist nicht nur Modellverhalten. Es ist eine Kombination aus:

- zu schwachen Runtime-Guards gegen bekannte Footguns
- unvollständiger Fehlertransparenz im Engine-/Run-Pfad
- Drift zwischen Help, Tips, Validator und tatsächlichem Runtime-Verhalten
- fehlender geführter Reparaturpfad für Agenten

## Zielbild

Der richtige Brix-Weg muss für LLMs der kürzeste und stabilste Weg sein.

- Der richtige Pfad ist klar und direkt.
- Der falsche Pfad wird früh geblockt oder explizit als Anti-Pattern markiert.
- Jeder Fehler ist lokal sichtbar: Step, Phase, Ursache, nächste Aktion.
- Produktbugs erzeugen Produktfixes, keine dauerhaften Pipeline-Workarounds.

## Leitprinzipien

### 1. Hard Guards statt weicher Warnungen

Bekannte gefährliche Muster dürfen nicht nur gewarnt werden.

- `db.query` darf DML nicht still akzeptieren.
- Legacy-Step-Types sollen in strikten Modi blockieren.
- direkte SQL-/YAML-/Container-Workarounds müssen in Brix als Anti-Pattern sichtbar sein.

### 2. Fehler auf die richtige Ebene surfacen

Ein fehlgeschlagener Run muss immer beantworten:

- letzter erfolgreicher Step
- erster nicht ausgeführter Step
- Fehlerphase: render, pre_execute, runner, persist, finalize
- Root Exception
- nächste empfohlene Aktion

### 3. Advice statt bloßes Meckern

Jeder relevante Validator-/Diagnose-Fund braucht:

- was falsch ist
- warum es falsch ist
- was der richtige Brix-Weg ist
- welches MCP-Tool als Nächstes aufgerufen werden sollte

### 4. Workaround-Resistenz als Produktfeature

Wenn ein bekannter Workaround möglich bleibt, wird er genutzt. Brix muss bekannte Footguns aktiv unattraktiv machen.

## Strategische Maßnahmen

## A. Runtime Guardrails

### A1. Dangerous Pattern Blocking

Runtime-Guards für bekannte Fehlmuster einführen:

- `db.query` + DML -> harter Laufzeitfehler mit Verweis auf `db.exec`
- bekannte Legacy-Aliase -> strict mode blockiert statt nur zu warnen
- offensichtliche Direktzugriffs-/Workaround-Muster -> als Anti-Pattern markieren

### A2. Strict Production Mode

Ein produktiver Modus für Buddy-/Produktpipelines:

- keine still geduldeten Footguns
- keine DML-via-`db.query`
- keine stillen Engine-Abbrüche ohne sichtbaren Fehler
- keine Legacy-Step-Typen

## B. Error Surfacing

### B1. Structured Run Failure Model

Runs brauchen zusätzliche strukturierte Fehlfelder:

- `failed_phase`
- `failed_step_id`
- `root_exception`
- `internal_error_code`
- `next_actions`

### B2. Synthetic Engine Error Anchors

Wenn die Engine außerhalb des normalen Step-Error-Pfads scheitert, muss ein sichtbarer Fehleranker persistiert werden:

- synthetischer Fehler-Step oder äquivalenter Run-Fehler
- sichtbar in `get_run_errors`, `get_run_log`, `diagnose_run`

## C. Guided Repair

### C1. Repair Pipeline Flow

Neuen geführten Reparaturpfad schaffen, z. B. `repair_pipeline`:

- Validator
- Brick-Schema-Abgleich
- letzter Run / Diagnose
- Anti-Pattern-Erkennung
- geordnete Reparaturagenda mit Confidence

### C2. Phase-Based Advice

Antworten sollen zwischen diesen Klassen unterscheiden:

- product bug
- config bug
- data issue
- external dependency issue
- drift/documentation issue

## D. Parity Across Surfaces

Help, Tips, Validator, Registry und Runtime müssen denselben Vertrag sprechen.

Pflicht-Parität für:

- Brick-Namen und Legacy-Status
- `db.query` vs `db.exec` Semantik
- Helper-vs-Brick Regeln
- DB-first / Brick-first Grundsätze

Ein Integrity-Fund wie `HELP_LEGACY_TYPE` ist daher kein kosmetischer Fehler, sondern Steuerungsdrift.

## E. Agent-Focused QA

Nicht nur Code testen, sondern typische Agent-Fehlpfade:

- Agent versucht DML über `db.query`
- Run endet mit `success: 0` und ohne sichtbare Fehler
- `db.exec` mit positional params im echten Engine-Pfad
- Agent versucht direkten SQL-/YAML-Workaround

## Umsetzungsphasen

## Phase 1: Stop The Bleeding

- Engine Error Surfacing fixen
- `db.exec` positional params im echten Engine-Pfad fixen
- `db.query` DML runtime-blocken

## Phase 2: Strong Guidance

- Advice Engine ausbauen
- Anti-Pattern-Katalog einführen
- `repair_pipeline` oder äquivalenten geführten Debug-Flow bauen

## Phase 3: Operational Discipline

- Strict Production Mode für Buddy aktivieren
- Buddy-Incident-Methodik festschreiben
- Help/Tips/Validator/Runtime-Parität regelmäßig prüfen

## Konkrete Arbeitsaufträge

1. Engine-Abbrüche müssen als sichtbare Fehler in Run-Diagnose und Run-History auftauchen.
2. Der Renderpfad für `db.exec` mit positional/list-params muss end-to-end stabilisiert werden.
3. `db.query` darf DML nicht mehr still oder scheinbar erfolgreich ausführen.
4. Ein Anti-Pattern-Katalog muss bekannte Workarounds explizit erkennen und benennen.
5. Ein geführter Reparaturpfad muss Agenten den nächsten korrekten Brix-Schritt zeigen.
6. Help, Tips, Validator und Runtime müssen denselben produktiven Vertrag ausdrücken.

## Erfolgskriterien

- Weniger produktive Workarounds in Pipelines
- kürzere Debug-Zeit pro Incident
- Produktbugs werden direkt als Produktbugs erkannt
- LLMs bleiben innerhalb des vorgesehenen Brix-Pfads
- Buddy-Sessions eskalieren nicht mehr in mehrtägige Trial-and-Error-Zyklen
