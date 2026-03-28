# Brix Cookbook

Praktische Use-Case-Beispiele mit der Brick-First-Architektur.

---

## Use Case 1: E-Mail-Anhänge verarbeiten

**Problem:** Du bekommst täglich Rechnungen per E-Mail. Jede hat einen PDF-Anhang. Du willst alle PDFs der letzten Woche automatisch in ein Verzeichnis laden und als Markdown-Text speichern — bereit für weitere Verarbeitung.

Ohne Brix: ~50 MCP-Calls, manuelle Filterung, kein Retry bei Fehlern.

**Pipeline:**

```yaml
name: verarbeite-rechnungs-anhaenge
description: Lade PDF-Anhänge aus Outlook-Rechnungsmails und konvertiere zu Markdown

input:
  ordner: { type: string, default: "Inbox" }
  suchbegriffe: { type: string, default: "Rechnung,Invoice" }
  ausgabe: { type: string, default: "/host/root/dev/rechnungen" }

steps:
  - id: holen
    type: source.fetch
    config:
      connector: outlook
      folder: "{{ input.ordner }}"
      filter: "hasAttachments:true"
      top: 100
      fetch_all_pages: true          # folgt Pagination automatisch

  - id: filtern
    type: flow.filter
    input: "{{ holen.output }}"
    config:
      where: >
        {{ input.suchbegriffe.split(',') | select('in', item.subject) | list | length > 0 }}

  - id: anhaenge
    type: mcp.call
    foreach: "{{ filtern.output }}"
    parallel: true
    concurrency: 5
    flatten: true
    config:
      server: m365
      tool: list-mail-attachments
      params:
        message_id: "{{ item.id }}"

  - id: nur_pdfs
    type: flow.filter
    input: "{{ anhaenge.output }}"
    config:
      where: "{{ item.name.endswith('.pdf') }}"

  - id: speichern
    type: script.python
    foreach: "{{ nur_pdfs.output }}"
    parallel: true
    concurrency: 5
    config:
      helper: save_attachment
      params:
        content: "{{ item.contentBytes }}"
        filename: "{{ item.name }}"
        output_dir: "{{ input.ausgabe }}"

  - id: konvertieren
    type: markitdown.convert
    foreach: "{{ speichern.output }}"
    parallel: true
    concurrency: 3
    config:
      input_path: "{{ item.path }}"
      output_dir: "{{ input.ausgabe }}/md"

  - id: bericht
    type: flow.aggregate
    input: "{{ konvertieren.output }}"
    config:
      group_by: "status"
      metrics: [count]
```

**Ausführen:**

```python
mcp__brix__run_pipeline(
    pipeline_id="verarbeite-rechnungs-anhaenge",
    input={
        "suchbegriffe": "Rechnung,Invoice,Abrechnung",
        "ausgabe": "/host/root/dev/rechnungen"
    }
)
```

**Erklärung:**

| Step | Brick | Was passiert |
|------|-------|-------------|
| `holen` | `source.fetch` | Holt bis zu 100 Mails aus Outlook, folgt automatisch der Pagination |
| `filtern` | `flow.filter` | Behält nur Mails deren Betreff einen Suchbegriff enthält |
| `anhaenge` | `mcp.call` | Holt Anhänge für jede Mail parallel (concurrency: 5), flatten=true → flache Liste |
| `nur_pdfs` | `flow.filter` | Filtert auf `.pdf`-Dateien |
| `speichern` | `script.python` | Speichert Binärinhalt auf Disk |
| `konvertieren` | `markitdown.convert` | Konvertiert PDF zu Markdown via MarkItDown-Service |
| `bericht` | `flow.aggregate` | Zählt erfolgreiche und fehlgeschlagene Konvertierungen |

`flatten: true` auf dem `anhaenge`-Step ist wichtig: ohne es wäre das Ergebnis eine Liste von Listen (eine pro Mail). Mit `flatten` entsteht eine flache Liste aller Anhänge.

---

## Use Case 2: Dokumente klassifizieren

**Problem:** Du hast 200 gescannte Dokumente auf OneDrive. Du weißt nicht was in jedem steckt — Rechnung, Vertrag, Versicherung, Sonstiges? Ein manuelles Durchschauen würde Stunden dauern.

**Pipeline:**

```yaml
name: klassifiziere-dokumente
description: Klassifiziert Dokumente per LLM und verschiebt sie in Unterordner

input:
  quell_ordner: { type: string, default: "/Dokumente/Eingang" }
  ausgabe_basis: { type: string, default: "/host/root/dev/sortiert" }
  batch_groesse: { type: integer, default: 20 }

steps:
  - id: dateien_holen
    type: source.fetch
    config:
      connector: onedrive
      folder: "{{ input.quell_ordner }}"
      extensions: [pdf, docx, png, jpg]

  - id: konvertieren
    type: markitdown.convert
    foreach: "{{ dateien_holen.output }}"
    parallel: true
    concurrency: 5
    on_error: continue                # einzelne Fehler nicht abbrechen
    config:
      input_path: "{{ item.local_path }}"

  - id: klassifizieren
    type: llm.batch
    input: "{{ konvertieren.output }}"
    config:
      prompt: |
        Klassifiziere dieses Dokument in genau eine Kategorie.
        Mögliche Kategorien: rechnung, vertrag, versicherung, bank, sonstiges

        Dokumentinhalt (Auszug):
        {{ item.markdown[:2000] }}

        Antworte NUR mit dem Kategorienamen, ohne Erklärung.
      output_field: kategorie
      categories: [rechnung, vertrag, versicherung, bank, sonstiges]
      batch_size: "{{ input.batch_groesse }}"

  - id: fehlende_herausfiltern
    type: flow.filter
    input: "{{ klassifizieren.output }}"
    config:
      where: "{{ item.kategorie is defined and item.kategorie != '' }}"

  - id: felder_extrahieren
    type: extract.specialist
    foreach: "{{ fehlende_herausfiltern.output }}"
    config:
      input_field: "markdown"
      extract:
        - name: datum
          method: regex
          pattern: "\\b(\\d{2}\\.\\d{2}\\.\\d{4})\\b"
          group: 1
        - name: betrag
          method: regex
          pattern: "(\\d+[,.]\\d{2})\\s*€"
          group: 1
          default: ""
      output_format: dict

  - id: speichern
    type: script.python
    foreach: "{{ felder_extrahieren.output }}"
    parallel: true
    concurrency: 10
    config:
      helper: save_classified
      params:
        source_path: "{{ item.source_path }}"
        kategorie: "{{ item.kategorie }}"
        datum: "{{ item.datum | default('unbekannt') }}"
        ausgabe_basis: "{{ input.ausgabe_basis }}"

  - id: zusammenfassung
    type: flow.aggregate
    input: "{{ speichern.output }}"
    config:
      group_by: kategorie
      metrics: [count]

  - id: benachrichtigen
    type: action.notify
    config:
      channel: "#dokumente"
      message: |
        Klassifizierung abgeschlossen: {{ dateien_holen.output | length }} Dokumente
        {% for gruppe in zusammenfassung.output %}
        - {{ gruppe.kategorie }}: {{ gruppe.count }}
        {% endfor %}
```

**Erklärung:**

`llm.batch` ist der Kernbrick hier. Er nimmt eine Liste von Items und sendet jeden mit dem konfigurierten Prompt an das LLM. Das Ergebnis wird im `output_field` jedes Items gespeichert — das ursprüngliche Item bleibt erhalten, das Feld `kategorie` kommt dazu.

`extract.specialist` ersetzt einen Python-Helper für die Feldextraktion. Statt 30 Zeilen Regex-Code: eine YAML-Konfiguration mit `method: regex` und dem Muster. Die Validierung via `checks` ist optional.

`flow.aggregate` nach dem Speichern gibt eine Übersicht: wie viele Dokumente pro Kategorie wurden verarbeitet. Der `action.notify`-Brick sendet das Ergebnis direkt nach Mattermost.

---

## Use Case 3: Daten aggregieren

**Problem:** Du hast Transaktionsdaten aus mehreren Quellen — PayPal, Sparkasse, manuelle CSV-Einträge. Du willst monatliche Ausgaben pro Kategorie sehen, Duplikate rausfiltern, und das Ergebnis als Bericht ausgeben.

**Pipeline:**

```yaml
name: aggregiere-transaktionen
description: Holt Transaktionen aus mehreren Quellen und erstellt Monatsauswertung

input:
  von: { type: string, description: "Startdatum YYYY-MM-DD" }
  bis: { type: string, description: "Enddatum YYYY-MM-DD" }
  ausgabe: { type: string, default: "/host/root/dev/berichte" }

dag: true   # DAG-Modus: unabhängige Steps laufen parallel

steps:
  - id: paypal_holen
    type: source.fetch
    config:
      connector: paypal
      date_from: "{{ input.von }}"
      date_to: "{{ input.bis }}"

  - id: sparkasse_holen
    type: source.fetch
    config:
      connector: sparkasse
      date_from: "{{ input.von }}"
      date_to: "{{ input.bis }}"

  - id: csv_lesen
    type: db.query
    config:
      sql: >
        SELECT id, datum, betrag, verwendungszweck, 'manuell' as quelle
        FROM transaktionen
        WHERE datum BETWEEN '{{ input.von }}' AND '{{ input.bis }}'

  # merge wartet automatisch auf alle drei (depends_on via DAG-Auflösung)
  - id: zusammenfuehren
    type: flow.merge
    depends_on: [paypal_holen, sparkasse_holen, csv_lesen]
    config:
      sources:
        - "{{ paypal_holen.output }}"
        - "{{ sparkasse_holen.output }}"
        - "{{ csv_lesen.output }}"

  - id: dedup
    type: flow.dedup
    input: "{{ zusammenfuehren.output }}"
    config:
      key: "{{ item.id }}"

  - id: klassifizieren
    type: llm.batch
    input: "{{ dedup.output }}"
    config:
      prompt: >
        Klassifiziere diese Transaktion:
        Betrag: {{ item.betrag }} EUR
        Verwendungszweck: {{ item.verwendungszweck }}
        Kategorie (nur eine): lebensmittel, wohnen, transport, versicherung,
        abonnement, technik, gesundheit, sonstiges
      output_field: kategorie
      batch_size: 50

  - id: monats_aggregation
    type: flow.aggregate
    input: "{{ klassifizieren.output }}"
    config:
      group_by: kategorie
      metrics: [count, sum]
      sum_field: betrag

  - id: top_ausgaben
    type: flow.filter
    input: "{{ monats_aggregation.output }}"
    config:
      where: "{{ item.sum > 0 }}"

  - id: sortieren
    type: flow.transform
    input: "{{ top_ausgaben.output }}"
    config:
      expression: >
        {{ input | sort(attribute='sum', reverse=true) }}

  - id: bericht_speichern
    type: script.python
    config:
      helper: generate_report
      params:
        daten: "{{ sortieren.output }}"
        von: "{{ input.von }}"
        bis: "{{ input.bis }}"
        ausgabe: "{{ input.ausgabe }}"
        gesamt: "{{ dedup.output | map(attribute='betrag') | sum }}"

  - id: in_db_schreiben
    type: db.upsert
    input: "{{ klassifizieren.output }}"
    config:
      table: transaktionen_klassifiziert
      key_field: id

  - id: benachrichtigen
    type: action.notify
    depends_on: [bericht_speichern, in_db_schreiben]
    config:
      message: >
        Monatsauswertung {{ input.von }} bis {{ input.bis }} fertig.
        {{ dedup.output | length }} Transaktionen,
        {{ sortieren.output[0].kategorie }}: {{ sortieren.output[0].sum }} EUR (größte Kategorie)
```

**Ausführen:**

```python
mcp__brix__run_pipeline(
    pipeline_id="aggregiere-transaktionen",
    input={
        "von": "2026-02-01",
        "bis": "2026-02-28",
        "ausgabe": "/host/root/dev/berichte"
    }
)
```

**Erklärung:**

`dag: true` ist der Schlüssel: Die drei Fetch-Steps (`paypal_holen`, `sparkasse_holen`, `csv_lesen`) laufen automatisch parallel weil sie keine Abhängigkeiten untereinander haben. `flow.merge` wartet auf alle drei via `depends_on`.

`flow.dedup` filtert Duplikate anhand eines Keys. Das ist wichtig wenn Transaktionen aus mehreren Quellen kommen und dieselbe ID tragen könnten.

`db.query` und `db.upsert` sprechen die Brix-interne SQLite-DB an. Für externe Datenbanken: `mcp.call` mit einem passenden DB-MCP-Server oder `script.python` mit eigenem Datenbankzugriff.

`flow.aggregate` mit `metrics: [count, sum]` und `sum_field: betrag` berechnet in einem Step was sonst eine ganze Gruppe SQL-Abfragen wäre.

---

## Wann welcher Brick?

| Aufgabe | Brick |
|---------|-------|
| Daten aus Outlook/Gmail/OneDrive holen | `source.fetch` |
| Liste filtern | `flow.filter` |
| Felder umbenennen / umstrukturieren | `flow.transform` |
| LLM klassifizieren / extrahieren | `llm.batch` |
| Regex-Extraktion ohne Python | `extract.specialist` |
| PDF/DOCX zu Markdown | `markitdown.convert` |
| HTTP-API ansprechen | `http.request` |
| MCP-Server-Tool aufrufen | `mcp.call` |
| SQL-Abfrage | `db.query` |
| Daten speichern | `db.upsert` |
| Notification senden | `action.notify` |
| Menschliche Freigabe | `action.approval` |
| Mehrere Quellen zusammenführen | `flow.merge` |
| Duplikate entfernen | `flow.dedup` |
| Gruppieren + Statistiken | `flow.aggregate` |
| Unterschiede finden | `flow.diff` |
| Warten / Polling | `flow.repeat` + `flow.wait` |
| Eigene Python-Logik | `script.python` |
| Shell-Kommando | `script.cli` |
