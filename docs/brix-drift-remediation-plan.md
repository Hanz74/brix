# Brix Drift Remediation Plan

Status: 2026-04-08  
Scope: Systemweite Drift außerhalb des Validators. Fokus auf DB-first, Brick-first, Help/Tips, Integrity, Bestandsdaten und Tool-Surface.

## 1. Ausgangslage

Die aktuelle Brix-Instanz zeigt keinen einzelnen isolierten Bug, sondern systemische Drift. Das äußert sich an mehreren Stellen gleichzeitig:

- Integrity-Funde im Bestand
- veraltete Help-Texte mit Legacy-Step-Typen
- widersprüchliche Aussagen zwischen `get_tips`, `get_help`, Code und Datenbankbestand
- Hybrid-Verhalten an Stellen, die eigentlich schon DB-first/Brick-first sein sollten

Das ist gefährlicher als ein normaler Defekt, weil es Vertrauen zerstört. Der Agent bekommt je nach Einstiegspunkt unterschiedliche Wahrheiten über dasselbe System.

## 2. Konkrete aktuelle Befunde

Aus den live aufgerufenen Brix-MCP-Tools ergeben sich bereits klare Probleme:

- `get_tips` meldet `NO_STEP_ROWS` für mindestens eine Pipeline.
- `get_tips` meldet mehrere `UNKNOWN_HELPER_REF`.
- mehrere Helper haben keine `description`.
- `get_help(topic="quick-start")` zeigt Beispiele mit Legacy-Typen wie `http` und `python`.
- `get_tips` predigt Brick-first/DB-only deutlich strenger als einige Hilfetexte und Teile des Bestands es tatsächlich einhalten.

## 3. Zielbild

Das Ziel ist nicht bloß “weniger Fehler”, sondern ein System, das in sich konsistent ist:

- eine Architektur-Linie
- ein gültiger Bestandszustand
- eine Hilfe, die dieselbe Wahrheit sagt wie das Runtime-System
- Integritätsprüfungen, die echte Policy-Verstöße früh sichtbar machen
- klar getrennte Migrationsreste vs. produktive Dauerpfade

## 4. Prinzipien

- DB-first ist nicht Marketing, sondern technische Invariante.
- Brick-first ist nicht Empfehlung, sondern Default-Surface.
- Hilfe darf keine Legacy-Nutzung normalisieren.
- Bestandsdaten sind Teil des Produkts und müssen dieselben Regeln erfüllen wie neue Daten.
- Jede Drift braucht einen Owner und einen testbaren Schließpunkt.

## 5. Arbeitsstränge

## 5.1 Bestands-Integrität

Hier geht es um alles, was bereits in der DB oder im Registry-Bestand kaputt oder inkonsistent ist:

- Pipelines ohne Step-Rows
- unbekannte Helper-Referenzen
- fehlende Pflicht-Metadaten
- tote oder halb migrierte Artefakte

## 5.2 Help- und Tips-Konsistenz

Hier geht es darum, dass `get_tips`, `get_help`, Quick-Start und Best Practices dieselbe Produktlinie vertreten:

- Brick-Namen statt Legacy-Typen
- DB-only CRUD statt YAML-/Bash-Denken
- aktuelle Tool-Namen und empfohlene Reihenfolge

## 5.3 Policy Enforcement

Hier geht es darum, die gewünschte Linie nicht nur zu dokumentieren, sondern technisch durchzusetzen:

- strengere Integrity-Checks
- stärkere Hinweise oder Warnungen bei Legacy-Nutzung
- klare Trennung von Kompatibilitätspfad und Zielpfad

## 5.4 Drift Detection

Hier geht es um Mechanismen, die neue Drift früh erkennen:

- Hilfe gegen Brick-Registry prüfen
- Example-Snippets gegen reale Typnamen prüfen
- gespeicherte Pipelines auf Legacy-/Migrationsreste prüfen

## 6. Cody-Style Task Plan

## T-DRIFT-001 Integrity Inventory

Goal:
Alle aktuell von `get_tips` und Integrity-Prüfungen gemeldeten Bestandsprobleme vollständig inventarisieren und kategorisieren.

Why:
Ohne vollständiges Inventar bleibt die Arbeit reaktiv. Dann wird immer nur der lauteste Fehler gefixt.

Deliverables:

- Liste aller aktuellen Integrity Findings
- Gruppierung nach Typ: missing rows, unknown helper refs, missing metadata, stale artifacts
- Zuordnung pro Entity: Pipeline, Helper, Trigger, Brick, Variable

Done when:

- es gibt ein belastbares Ist-Bild
- jede Finding-Klasse hat eine Anzahl und konkrete betroffene Objekte

## T-DRIFT-002 Pipeline Row Repair

Goal:
Pipelines mit beschädigter oder unvollständiger Step-Persistenz bereinigen.

Why:
Eine Pipeline ohne `pipeline_step`-Rows ist ein DB-first Bruch auf Persistenzebene.

Deliverables:

- Analyse, wie `NO_STEP_ROWS` entstanden ist
- Reparaturpfad für betroffene Pipelines
- Schutz gegen erneutes Speichern unvollständiger Pipelines

Done when:

- kein produktiver Pipeline-Eintrag mehr ohne Step-Rows existiert
- ein Regressionstest verhindert erneutes Auftreten

## T-DRIFT-003 Helper Reference Cleanup

Goal:
Unbekannte Helper-Referenzen im Bestand auflösen oder entfernen.

Why:
Ein gespeicherter Step mit totem Helper-Verweis ist ein stiller Betriebsfehler im System.

Deliverables:

- Mapping aller `UNKNOWN_HELPER_REF`-Vorkommen
- Entscheidung pro Fall: Helper re-register, Step umstellen, Referenz entfernen
- Integritätsregel für künftige Helper-Löschungen oder Renames

Done when:

- keine produktiven Steps mehr auf unbekannte Helper zeigen

## T-DRIFT-004 Metadata Backfill

Goal:
Fehlende `description`-, `project`- oder `tags`-Metadaten in Bestandsobjekten bereinigen.

Why:
Ohne Metadaten verlieren Discoverability, Governance und Tool-Suggestions an Qualität.

Deliverables:

- Backfill für fehlende Descriptions
- Policy, welche Felder Pflicht sind
- Bestandsprüfung für neue Entitäten

Done when:

- alle produktiven Helpers und Pipelines haben sinnvolle Basis-Metadaten

## T-DRIFT-005 Help Surface Audit

Goal:
Alle Help-Topics und Quick-Start-Inhalte gegen die aktuelle Brick-first/DB-first-Linie prüfen.

Why:
Wenn Help veraltete Patterns normalisiert, produziert das System selbst neue Fehlkonfigurationen.

Deliverables:

- Audit aller relevanten Topics
- Liste veralteter Beispiele, Legacy-Typnamen und falscher Empfehlungen
- Priorisierung nach Nutzerwirkung

Done when:

- klar ist, welche Topics produktionsrelevant falsch oder veraltet sind

## T-DRIFT-006 Quick-Start Rewrite

Goal:
`quick-start` und zentrale Hilfetexte auf aktuelle Brick-Namen und DB-only-Workflows umstellen.

Why:
Das ist die sichtbarste Drift im aktuellen System.

Deliverables:

- neue minimal richtige Quick-Start-Beispiele
- Brick-first Typnamen
- richtige MCP-Tool-Namen und Ablaufreihenfolge

Done when:

- `get_help("quick-start")` keine Legacy-Typen mehr normalisiert
- das erste Beispiel dem Zielsystem entspricht

## T-DRIFT-007 Tips/Help Policy Alignment

Goal:
`get_tips` und `get_help` in Ton, Regelwerk und Prioritäten angleichen.

Why:
Der Nutzer darf von beiden Oberflächen dieselbe normative Antwort bekommen.

Deliverables:

- definierte Policy-Statements
- Abgleich von Verbotslisten, Empfehlungen und Reihenfolgen
- Entscheidung, was hart, weich oder rein informativ ist

Done when:

- Tips und Help sich nicht mehr gegenseitig widersprechen

## T-DRIFT-008 Legacy Surface Classification

Goal:
Alle verbleibenden Legacy-Pfade klassifizieren: kompatibel, deprecated, intern-only oder zu entfernen.

Why:
Ohne Klassifikation bleibt “legacy” ein unendlicher Graubereich.

Deliverables:

- Liste aller Legacy-Step-Typen, alten Beispiele und Kompatibilitätspfade
- Entscheidung pro Fall
- sichtbare Policy für Nutzer und intern für Maintainer

Done when:

- jede relevante Legacy-Fläche hat einen expliziten Status

## T-DRIFT-009 Drift Detection Checks

Goal:
Automatische Prüfungen einführen, die neue Hilfe-/Schema-/Bestands-Drift früh erkennen.

Why:
Sonst wird das System nach jeder größeren Änderung wieder auseinanderlaufen.

Deliverables:

- Test oder Integrity-Check für Help-Beispiele gegen echte Brick-Namen
- Check für Legacy-Typen in produktionsnahen Help-Topics
- Check für gespeicherte Pipelines gegen unbekannte Brick-/Helper-Refs

Done when:

- neue Drift wird durch Tests oder Integrity-Checks sichtbar, bevor sie Nutzer trifft

## T-DRIFT-010 Runtime Policy Escalation

Goal:
Aus weichen Architekturregeln dort technische Enforcement-Regeln machen, wo das System sonst immer wieder kippt.

Why:
Nur dokumentierte Regeln verlieren gegen operative Bequemlichkeit.

Deliverables:

- definierte Eskalation: info -> warning -> error
- Entscheidung für `strict_bricks`-ähnliche Policy-Felder in weiteren Flächen
- klare Ausnahmefälle für Kompatibilität

Done when:

- zentrale Architekturregeln können nicht mehr stillschweigend unterlaufen werden

## 7. Empfohlene Reihenfolge

Zuerst die Bestandswahrheit stabilisieren, dann die Hilfe korrigieren, erst danach Enforcement verschärfen.

Empfohlener Ablauf:

1. T-DRIFT-001
2. T-DRIFT-002
3. T-DRIFT-003
4. T-DRIFT-004
5. T-DRIFT-005
6. T-DRIFT-006
7. T-DRIFT-007
8. T-DRIFT-008
9. T-DRIFT-009
10. T-DRIFT-010

## 8. Definition of Done

Die Drift-Arbeit ist erst dann wirklich abgeschlossen, wenn:

- `get_tips` keine bekannten Bestandsfehler mehr meldet, die eigentlich reparierbar sind
- `get_help("quick-start")` ausschließlich aktuelle Brick-first/DB-only-Pfade zeigt
- produktive Pipelines und Helper konsistente Metadaten haben
- Legacy-Pfade klar klassifiziert sind
- mindestens ein automatischer Drift-Check neue Inkonsistenzen erkennt

## 9. Harte Regel

Jede Änderung an Hilfe, Brick-Namen, Registry, Persistenz oder Integritätslogik braucht künftig nicht nur Code-Tests, sondern auch einen Konsistenztest gegen die öffentliche MCP-Oberfläche.  
Wenn `get_tips`, `get_help`, Registry und Runtime nicht dieselbe Geschichte erzählen, ist die Änderung unvollständig.
