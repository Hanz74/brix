# Runner Output Contracts

This document defines what `{{ step.output }}` contains for every runner in Brix.

The pipeline engine extracts the `"data"` key from each runner's return dict and exposes it as `{{ step_id.output }}` in Jinja2 templates. Additional top-level keys returned by runners (e.g. `status_code`, `mcp_trace`, `stderr`) are stored internally but are **not** accessible via `{{ step.output }}`.

---

## Table of Contents

- [http.request (HttpRunner)](#httprequest-httprunner)
- [mcp.call (McpRunner)](#mcpcall-mcprunner)
- [script.python (PythonRunner)](#scriptpython-pythonrunner)
- [script.cli (CliRunner)](#scriptcli-clirunner)
- [db.query (DbQueryRunner)](#dbquery-dbqueryrunner)
- [db.upsert (DbUpsertRunner)](#dbupsert-dbupsertrunner)
- [llm.batch (LlmBatchRunner)](#llmbatch-llmbatchrunner)
- [source.fetch (SourceRunner)](#sourcefetch-sourcerunner)
- [markitdown.convert (MarkitdownRunner)](#markitdownconvert-markitdownrunner)
- [extract.specialist (SpecialistRunner)](#extractspecialist-specialistrunner)
- [flow.filter (FilterRunner)](#flowfilter-filterrunner)
- [flow.transform (TransformRunner)](#flowtransform-transformrunner)
- [flow.aggregate (AggregateRunner)](#flowaggregate-aggregaterunner)
- [flow.set (SetRunner)](#flowset-setrunner)
- [flow.choose (ChooseRunner)](#flowchoose-chooserunner)
- [flow.switch (SwitchRunner)](#flowswitch-switchrunner)
- [flow.merge (MergeRunner)](#flowmerge-mergerunner)
- [flow.flatten (FlattenRunner)](#flowflatten-flattenrunner)
- [flow.dedup (DedupRunner)](#flowdedup-deduprunner)
- [flow.diff (DiffRunner)](#flowdiff-diffrunner)
- [flow.repeat (RepeatRunner)](#flowrepeat-repeatrunner)
- [flow.wait (WaitRunner)](#flowwait-waitrunner)
- [flow.validate (ValidateRunner)](#flowvalidate-validaterunner)
- [flow.parallel (ParallelStepRunner)](#flowparallel-parallelsteprunner)
- [flow.error_handler (ErrorHandlerRunner)](#flowerror_handler-errorhandlerrunner)
- [flow.pipeline (PipelineRunner)](#flowpipeline-pipelinerunner)
- [flow.pipeline_group (PipelineGroupRunner)](#flowpipeline_group-pipelinegrouprunner)
- [action.notify (NotifyRunner)](#actionnotify-notifyrunner)
- [action.approval (ApprovalRunner)](#actionapproval-approvalrunner)
- [action.emit (EmitRunner)](#actionemit-emitrunner)
- [action.respond (RespondRunner)](#actionrespond-respondrunner)
- [action.queue (QueueRunner)](#actionqueue-queuerunner)

---

## http.request (HttpRunner)

**Output-Typ:** Any (dict, list, or string depending on response Content-Type)

**Behaviour:** JSON responses are auto-parsed into dict/list. Non-JSON responses are returned as a raw string. When `fetch_all_pages: true` is set, the runner follows OData `@odata.nextLink` and RFC 5988 `Link` headers to collect all pages.

**Standard response (single page):**

The `data` field contains the parsed response body directly. It can be a dict, a list, or a string.

```json
{"status": "ok", "items": [1, 2, 3]}
```

**Paginated response (fetch_all_pages: true):**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| value | list | All items collected across all pages |
| _pages | int | Number of pages fetched |
| _total | int | Total number of items collected |

```json
{"value": [{"id": 1}, {"id": 2}, {"id": 3}], "_pages": 3, "_total": 3}
```

**Hinweis:** Bei JSON-Responses wird der Body automatisch geparst. Bei Nicht-JSON ist `{{ step.output }}` ein roher String. Bei HTTP-Fehler (>= 400) ist `success=false` und kein `data`-Feld vorhanden.

**Extra Top-Level Keys (nicht in step.output):**
- `status_code` (int) -- bei Fehler-Responses (>= 400) oder Rate-Limiting (429/503)
- `retry_after` (int) -- Sekunden bis zum Retry bei Rate-Limiting
- `rate_limited` (bool) -- true wenn Rate-Limited

---

## mcp.call (McpRunner)

**Output-Typ:** Any (dict, list, or string depending on the MCP tool's response)

**Felder:** Abhaengig vom aufgerufenen MCP-Tool. Der Runner parst die Textbloecke der MCP-Response als JSON; schlaegt das fehl, wird der rohe String zurueckgegeben.

```json
{"result": "some data from the MCP tool"}
```

**Hinweis:** Wenn `unwrap_json: true` auf dem Server oder Step konfiguriert ist, werden verschachtelte JSON-Strings automatisch entpackt (z.B. `{"result": "{\"inner\": true}"}` wird zu `{"result": {"inner": true}}`). Bei MCP-Tools, die `structuredContent` zurueckgeben (MCP spec >= 2025-06-18), wird dieses direkt als `data` verwendet.

**Extra Top-Level Keys (nicht in step.output):**
- `mcp_trace` (dict) -- Strukturiertes Trace-Record mit: `server`, `tool`, `arguments_summary`, `response_summary`, `duration`, `status`

---

## script.python (PythonRunner)

**Output-Typ:** Any (abhaengig vom stdout-Output des Scripts)

**Behaviour:** Der Runner fuehrt ein Python-Script als Subprocess aus. Stdout wird als JSON geparst; schlaegt das fehl, wird der rohe String zurueckgegeben.

```json
{"extracted": 42, "items": ["a", "b"]}
```

**Hinweis:** Das Script muss sein Ergebnis als JSON nach stdout schreiben (`print(json.dumps(result))`). Parameters werden als JSON-String in `sys.argv[1]` uebergeben (oder via stdin bei >100KB). Pipeline-Credentials sind als Umgebungsvariablen verfuegbar.

**Extra Top-Level Keys (nicht in step.output):**
- `stderr` (string) -- Stderr-Output des Scripts (ohne BRIX_PROGRESS-Zeilen falls `progress: true`)

---

## script.cli (CliRunner)

**Output-Typ:** Any (abhaengig vom stdout-Output des Befehls)

**Behaviour:** Fuehrt einen Shell-Befehl aus. Stdout wird als JSON geparst; schlaegt das fehl, wird der rohe String zurueckgegeben. Zwei Modi: `args` (Liste, shell=False) oder `command` (String, shell=True).

```json
{"key": "value"}
```

oder als roher String:

```
"some command output text"
```

**Hinweis:** Bei `exit code != 0` ist `success=false` und stderr wird als Error zurueckgegeben.

---

## db.query (DbQueryRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| rows | list[dict] | Liste der Ergebnis-Zeilen als Dicts (Spaltenname -> Wert) |
| row_count | int | Anzahl der zurueckgegebenen Zeilen |
| columns | list[str] | Spaltennamen in der Reihenfolge des Cursors |

```json
{
  "rows": [
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"}
  ],
  "row_count": 2,
  "columns": ["id", "name", "email"]
}
```

**Hinweis:** Unterstuetzt SQLite und PostgreSQL. Query wird als Jinja2-Template gerendert. Fuer Werte immer `params` mit Named Placeholders nutzen (SQL-Injection-sicher).

---

## db.upsert (DbUpsertRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| inserted | int | Anzahl eingefuegter Zeilen |
| updated | int | Anzahl aktualisierter Zeilen (Approximation) |
| total | int | inserted + updated |

```json
{"inserted": 5, "updated": 0, "total": 5}
```

**Hinweis:** `inserted` und `updated` sind Approximationen. Fuer exakte Zahlen auf `total` vertrauen. Bei `conflict_key` wird ein UPSERT ausgefuehrt (PostgreSQL: ON CONFLICT DO UPDATE, SQLite: INSERT OR REPLACE).

---

## llm.batch (LlmBatchRunner)

**Output-Typ:** list[dict]

**Felder pro Item:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| custom_id | string | ID des Items (aus `id` oder `custom_id` des Input-Items, oder auto-generiert) |
| result | Any | Geparstes LLM-Ergebnis (JSON wenn moeglich, sonst String) |
| usage | dict | Token-Usage mit `prompt_tokens`, `completion_tokens`, `total_tokens` |
| error | string/null | Fehlermeldung falls dieses Item fehlschlug |

```json
[
  {
    "custom_id": "item-0",
    "result": {"category": "invoice", "confidence": 0.95},
    "usage": {"prompt_tokens": 120, "completion_tokens": 30, "total_tokens": 150}
  },
  {
    "custom_id": "item-1",
    "result": {"category": "receipt", "confidence": 0.88},
    "usage": {"prompt_tokens": 115, "completion_tokens": 28, "total_tokens": 143}
  }
]
```

**Hinweis:** Nutzt die Mistral Batch API. Markdown-Code-Fences werden automatisch entfernt. Bei `output_schema` wird Structured Output erzwungen. Env-Vars: `BUDDY_LLM_API_KEY` oder `MISTRAL_API_KEY`.

**Extra Top-Level Keys (nicht in step.output):**
- `total` (int) -- Gesamtzahl der verarbeiteten Items

---

## source.fetch (SourceRunner)

**Output-Typ:** list[dict]

Jedes Item ist ein NormalizedItem-Dict mit folgenden Feldern:

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| source | string | Connector-Name (z.B. "local_files", "outlook") |
| source_type | string | Typ der Quelle (z.B. "file_storage", "email") |
| item_id | string | Eindeutige ID des Items |
| title | string | Dateiname oder E-Mail-Betreff |
| content | string/null | Inhalt (z.B. E-Mail-Body) |
| metadata | dict | Connector-spezifische Metadaten |
| attachments | list | Liste von Attachment-Referenzen |
| timestamp | string | ISO 8601 Zeitstempel |
| raw | dict | Rohe Quelldaten (Original-Response) |

**Beispiel (local_files):**
```json
[
  {
    "source": "local_files",
    "source_type": "file_storage",
    "item_id": "/host/root/data/invoice.pdf",
    "title": "invoice.pdf",
    "content": null,
    "metadata": {
      "path": "/host/root/data/invoice.pdf",
      "relative_path": "invoice.pdf",
      "size": 45230,
      "extension": "pdf"
    },
    "attachments": [],
    "timestamp": "2026-03-15T10:30:00+00:00",
    "raw": {"path": "/host/root/data/invoice.pdf", "stat": {"size": 45230, "mtime": 1742034600.0}}
  }
]
```

**Beispiel (outlook):**
```json
[
  {
    "source": "outlook",
    "source_type": "email",
    "item_id": "AAMk...",
    "title": "Rechnung März 2026",
    "content": "<html>...</html>",
    "metadata": {
      "from": "sender@example.com",
      "to": [{"emailAddress": {"address": "me@example.com"}}],
      "isRead": false,
      "hasAttachments": true,
      "importance": "normal",
      "categories": []
    },
    "attachments": [{"note": "fetch via list-mail-attachments", "messageId": "AAMk..."}],
    "timestamp": "2026-03-15T08:00:00+00:00",
    "raw": {"id": "AAMk...", "subject": "Rechnung März 2026", "...": "..."}
  }
]
```

---

## markitdown.convert (MarkitdownRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| markdown | string | Konvertierter Markdown-Text |
| metadata | dict | Dokument-Metadaten (vom markitdown-Service) |
| extracted | dict | Extrahierte Felder (nur bei `auto_extract: true`) |

```json
{
  "markdown": "# Rechnung\n\nBetrag: 42,00 EUR\n...",
  "metadata": {"pages": 2, "title": "Rechnung"},
  "extracted": {}
}
```

**Hinweis:** Nutzt den markitdown-mcp HTTP-Service. Input kann ein Dateipfad oder base64-kodierter Inhalt sein. Bei `auto_extract: true` wird `/v1/extract` statt `/v1/convert` verwendet.

---

## extract.specialist (SpecialistRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| result | dict/list/dict(flat) | Extrahierte Felder im gewaehlten `output_format` |
| warnings | list[string] | Validierungs-Warnungen (on_fail=warn) |
| skipped | bool | true wenn eine Validierungsregel on_fail=skip ausgeloest hat |

**output_format Varianten:**
- `dict` (default): `{"field_name": value, ...}`
- `list`: `[value1, value2, ...]` (Reihenfolge der Extraction Rules)
- `flat`: `{"field_name": value, ...}` wobei Listen zu kommaseparierten Strings werden

```json
{
  "result": {
    "invoice_number": "RE-2026-001",
    "amount": "42.00",
    "currency": "EUR"
  },
  "warnings": [],
  "skipped": false
}
```

**Hinweis:** Unterstuetzt vier Extraktionsmethoden: `regex`, `json_path`, `split`, `template`. Validierungsregeln koennen `required`, `min_length`, `max_length`, `regex`, `type` pruefen.

---

## flow.filter (FilterRunner)

**Output-Typ:** list

Die gefilterte Liste (gleicher Item-Typ wie Input).

```json
[
  {"name": "invoice.pdf", "size": 1024},
  {"name": "receipt.pdf", "size": 512}
]
```

**Hinweis:** Jedes Item wird gegen den `where`-Ausdruck (Jinja2) evaluiert. Items bei denen der Ausdruck zu "false", "0", "" oder "none" rendert, werden entfernt. Items mit Expression-Fehlern werden uebersprungen.

**Extra Top-Level Keys (nicht in step.output):**
- `items_count` (int) -- Anzahl der Items nach Filterung

---

## flow.transform (TransformRunner)

**Output-Typ:** Any (abhaengig vom Input-Typ und Expression)

**Behaviour:**
- **List-Input:** Expression wird pro Item angewendet (Variable `item`). Ergebnis ist eine Liste.
- **Dict-Input:** Expression wird einmal angewendet (Variable `data`). Ergebnis ist ein einzelner Wert.
- **Sonstiges:** Expression wird einmal angewendet (Variable `value`). Ergebnis ist ein String.

Jedes gerenderte Ergebnis wird als JSON geparst; schlaegt das fehl, bleibt es ein String.

```json
["Alice Smith", "Bob Jones"]
```

---

## flow.aggregate (AggregateRunner)

**Output-Typ:** dict

Ein Dict, das nach dem `group_by`-Wert keyed ist. Jeder Wert ist ein Dict mit den berechneten Aggregations-Operationen.

**Unterstuetzte Operationen:** `sum`, `count`, `min`, `max`, `avg`, `collect`

```json
{
  "groceries": {
    "total_amount": 42.5,
    "count": 3,
    "names": ["Milk", "Bread", "Butter"]
  },
  "electronics": {
    "total_amount": 599.99,
    "count": 1,
    "names": ["Monitor"]
  }
}
```

**Extra Top-Level Keys (nicht in step.output):**
- `group_count` (int) -- Anzahl der Gruppen

---

## flow.set (SetRunner)

**Output-Typ:** dict

Gibt die (bereits durch die Engine gerenderten) Key/Value-Paare direkt zurueck.

```json
{
  "greeting": "Hello Alice",
  "count": "42",
  "today": "2026-03-29"
}
```

**Hinweis:** Werte sind immer Strings (Jinja2-gerendert). Bei `persist: true` werden die Werte zusaetzlich in die `persistent_store` DB-Tabelle geschrieben.

---

## flow.choose (ChooseRunner)

**Output-Typ:** Any (abhaengig vom ausgefuehrten Branch)

Gibt das Ergebnis des ersten passenden Branch zurueck (oder `null` wenn kein Branch und kein Default matcht).

```json
{"status": "processed", "count": 5}
```

**Hinweis:** Die `when`-Bedingungen werden in Reihenfolge evaluiert. Der erste true-Branch wird als Mini-Pipeline ausgefuehrt.

---

## flow.switch (SwitchRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| matched_case | string/null | Der gematchte Case-Wert, oder null bei Default |
| target_step | string | ID des Ziel-Steps |
| evaluated_value | string | Der evaluierte Wert des `field`-Ausdrucks |

```json
{
  "matched_case": "approved",
  "target_step": "step_approve",
  "evaluated_value": "approved"
}
```

**Hinweis:** Switch evaluiert den `field`-Ausdruck und vergleicht das Ergebnis als String gegen die `cases`-Keys. Die Engine nutzt `target_step` fuer Branching-Entscheidungen.

---

## flow.merge (MergeRunner)

**Output-Typ:** list[dict]

Eine zusammengefuehrte Liste aus den Outputs der referenzierten Steps.

**Modi:**
- **append:** Alle Listen hintereinander konkateniert.
- **zip:** Items positional zusammengefuehrt (kuerzere Listen mit leeren Dicts gepadded).
- **lookup:** Left-Join der ersten Liste gegen die restlichen ueber einen `key`.

```json
[
  {"id": 1, "name": "Alice", "order_count": 5},
  {"id": 2, "name": "Bob", "order_count": 3}
]
```

---

## flow.flatten (FlattenRunner)

**Output-Typ:** list

Die geflattete Liste.

```json
[1, 2, 3, 4, 5]
```

**Hinweis:** `depth: 1` (default) flattet eine Ebene. `depth: -1` flattet unbegrenzt. Bei `field` wird zuerst das angegebene Feld aus jedem Item extrahiert und dann geflatteted.

**Extra Top-Level Keys (nicht in step.output):**
- `items_count` (int) -- Anzahl der Items nach Flattening

---

## flow.dedup (DedupRunner)

**Output-Typ:** list

Die deduplizierte Liste (gleicher Item-Typ wie Input).

```json
[
  {"email": "alice@example.com", "name": "Alice"},
  {"email": "bob@example.com", "name": "Bob"}
]
```

**Hinweis:** `key` ist ein Jinja2-Ausdruck der pro Item den Dedup-Schluessel berechnet. `keep: first` (default) behaelt das erste Vorkommen, `keep: last` das letzte.

**Extra Top-Level Keys (nicht in step.output):**
- `items_count` (int) -- Anzahl Items nach Dedup
- `original_count` (int) -- Anzahl Items vor Dedup

---

## flow.diff (DiffRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| added | list[dict] | Items in `right` aber nicht in `left` |
| removed | list[dict] | Items in `left` aber nicht in `right` |
| changed | list[dict] | Items in beiden mit unterschiedlichen Werten |
| unchanged | list[dict] | Items in beiden mit identischen Werten |

Jedes `changed`-Item hat die Form: `{"key": <key_value>, "left": <old_item>, "right": <new_item>}`

```json
{
  "added": [{"id": 3, "name": "Charlie"}],
  "removed": [{"id": 1, "name": "Alice"}],
  "changed": [{"key": 2, "left": {"id": 2, "name": "Bob"}, "right": {"id": 2, "name": "Robert"}}],
  "unchanged": []
}
```

**Extra Top-Level Keys (nicht in step.output):**
- `summary` (dict) -- `{"added": int, "removed": int, "changed": int, "unchanged": int}`

---

## flow.repeat (RepeatRunner)

**Output-Typ:** Any (Ergebnis der letzten Iteration)

Gibt `result` der letzten ausgefuehrten Mini-Pipeline zurueck (das Ergebnis der letzten `sequence`-Ausfuehrung).

```json
{"status": "ready", "retries": 3}
```

**Hinweis:** Sub-Step-Outputs werden in den Parent-Context gemerged, sodass `until`/`while_condition` darauf zugreifen koennen. Bei erschoepftem `max_iterations` ohne erfuellte `until`-Bedingung ist `success=false`.

**Extra Top-Level Keys (nicht in step.output):**
- `iterations` (int) -- Anzahl ausgefuehrter Iterationen
- `exhausted` (bool) -- true wenn max_iterations erreicht ohne until-Bedingung

---

## flow.wait (WaitRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| waited_seconds | float | Tatsaechlich gewartete Sekunden |
| condition_met | bool/null | true wenn until-Bedingung erfuellt, null bei fixem Delay |
| timed_out | bool | true wenn Timeout erreicht wurde |
| poll_count | int | Anzahl der Polling-Zyklen (nur bei until-Modus) |

```json
{
  "waited_seconds": 12.3,
  "condition_met": true,
  "timed_out": false,
  "poll_count": 3
}
```

**Hinweis:** Zwei Modi: Fixer Delay (`seconds: 30`) oder Condition Polling (`until: "{{ ... }}"`). Bei Timeout ist `success=true` aber `timed_out=true`.

---

## flow.validate (ValidateRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| violations | list[dict] | Regeln mit on_fail=stop die fehlgeschlagen sind |
| warnings | list[dict] | Regeln mit on_fail=warn die fehlgeschlagen sind |

Jeder Eintrag in violations/warnings enthaelt:

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| rule | dict | Die Original-Regel |
| passed | int | Anzahl Items die bestanden haben |
| total | int | Gesamtzahl der evaluierten Items |
| ratio | float | passed / total |
| min_ratio | float | Geforderte Mindest-Quote |

```json
{
  "violations": [],
  "warnings": [
    {
      "rule": {"field": "{{ item.email }}", "min_ratio": 0.95, "of": "{{ users.output }}", "on_fail": "warn"},
      "passed": 90,
      "total": 100,
      "ratio": 0.9,
      "min_ratio": 0.95
    }
  ]
}
```

**Hinweis:** Bei `on_fail: stop` und mindestens einer Violation ist `success=false`.

---

## flow.parallel (ParallelStepRunner)

**Output-Typ:** dict

Ein Dict keyed nach Sub-Step-IDs. Jeder Wert ist das `result` der jeweiligen Mini-Pipeline.

```json
{
  "fetch_users": [{"id": 1, "name": "Alice"}],
  "fetch_orders": [{"id": 101, "total": 42.0}]
}
```

**Hinweis:** `success` ist nur true wenn ALLE Sub-Steps erfolgreich waren. `concurrency` begrenzt die maximale Parallelitaet.

---

## flow.error_handler (ErrorHandlerRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| success | bool | true wenn try_step erfolgreich war |
| result | Any | Output des ausgefuehrten Steps (try oder handler) |
| error | string/null | Fehlermeldung des try_step (null bei Erfolg) |
| used_handler | bool | true wenn der handler_step ausgefuehrt wurde |

```json
{
  "success": false,
  "result": {"fallback_data": [1, 2, 3]},
  "error": "HTTP 503: Service unavailable",
  "used_handler": true
}
```

**Hinweis:** Der aeussere `success` ist true solange entweder try_step oder handler_step erfolgreich war. Nur wenn auch der handler_step fehlschlaegt, wird `success=false` auf der aeusseren Ebene gesetzt.

---

## flow.pipeline (PipelineRunner)

**Output-Typ:** Any (Ergebnis der Sub-Pipeline)

Gibt das `result` der Sub-Pipeline zurueck (typischerweise das Output des letzten Steps in der Sub-Pipeline).

```json
{"processed": true, "items_count": 42}
```

**Hinweis:** Maximale Verschachtelungstiefe: 10. Sub-Pipeline wird ueber Pfad oder Name aufgeloest (`~/.brix/pipelines/`).

**Extra Top-Level Keys (nicht in step.output):**
- `slots` (dict) -- Evaluierte `output_slots` der Sub-Pipeline (benannte Output-Mappings)

---

## flow.pipeline_group (PipelineGroupRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| results | dict | Pipeline-Name -> Result-Data (oder null bei Fehler) |
| errors | dict | Pipeline-Name -> Fehlermeldung (nur fuer fehlgeschlagene) |
| total | int | Gesamtzahl der Pipelines |
| succeeded | int | Anzahl erfolgreicher Pipelines |
| failed | int | Anzahl fehlgeschlagener Pipelines |

```json
{
  "results": {
    "process_invoices": {"count": 10},
    "process_receipts": {"count": 5},
    "process_contracts": null
  },
  "errors": {
    "process_contracts": "Sub-pipeline failed: process_contracts"
  },
  "total": 3,
  "succeeded": 2,
  "failed": 1
}
```

**Hinweis:** Pipelines werden parallel mit `concurrency`-Limit (default: 3) ausgefuehrt. `shared_params` werden an alle Sub-Pipelines weitergereicht.

---

## action.notify (NotifyRunner)

**Output-Typ:** dict (abhaengig vom Kanal)

**Kanal "whatsapp":** Delegiert an McpRunner -- Output entspricht dem MCP-Tool-Ergebnis.

**Kanal "slack":** Delegiert an HttpRunner -- Output entspricht dem HTTP-Response.

**Kanal "log" (default/fallback):**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| channel | string | Genutzter Kanal (z.B. "log") |
| to | string | Empfaenger-Adresse |
| message | string | Gesendete Nachricht |
| status | string | Immer "logged" |

```json
{
  "channel": "log",
  "to": "",
  "message": "Pipeline completed successfully",
  "status": "logged"
}
```

---

## action.approval (ApprovalRunner)

**Output-Typ:** dict

**Bei Genehmigung:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| approved | bool | true |
| approved_by | string | Wer genehmigt hat (aus approval_pending.json) |

**Bei Ablehnung (success=false):**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| approved | bool | false |
| reason | string | Ablehnungsgrund |

**Bei Timeout mit on_timeout=continue:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| approved | bool | false |
| reason | string | "timeout" |
| auto_continued | bool | true |

```json
{"approved": true, "approved_by": "admin@example.com"}
```

**Hinweis:** Schreibt `approval_pending.json` ins Run-Workdir. Der REST-Endpoint `POST /approve/{run_id}` kann den Status aendern. Optional wird eine Benachrichtigung ueber WhatsApp/Slack gesendet.

---

## action.emit (EmitRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| event_id | string | UUID des emittierten Events |
| event_name | string | Name des Events (z.B. "order.received") |
| emitted_at | string | ISO 8601 Zeitstempel |
| data | Any | Die emittierten Daten |

```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "event_name": "order.received",
  "emitted_at": "2026-03-29T14:30:00+00:00",
  "data": {"order_id": 12345, "amount": 99.99}
}
```

**Hinweis:** Events werden in der `event_bus` DB-Tabelle persistiert. Konsum ueber `consume_events()` oder zukuenftige Event-Trigger.

---

## action.respond (RespondRunner)

**Output-Typ:** dict

**Felder:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| status | int | HTTP Status-Code (default: 200) |
| headers | dict | Response-Headers |
| body | string | Response-Body (Jinja2-gerendert) |
| responded | bool | Immer true |

```json
{
  "status": 200,
  "headers": {"Content-Type": "application/json"},
  "body": "{\"ok\": true, \"count\": 42}",
  "responded": true
}
```

**Hinweis:** Designed fuer Webhook-Endpoints. Der Body wird als Jinja2-Template mit dem Pipeline-Context gerendert.

---

## action.queue (QueueRunner)

**Output-Typ:** dict

**Wenn Buffer NICHT voll (waiting):**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| buffered | int | Aktuelle Anzahl Items im Buffer |
| threshold | int/null | Schwellwert fuer Flush (null bei zeitbasiert) |
| waiting | bool | true |
| queue_name | string | Name der Queue |

**Wenn Buffer geflushed:**

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| items | list | Alle gesammelten Items |
| flushed | int | Anzahl geflushter Items |
| waiting | bool | false |
| queue_name | string | Name der Queue |
| flush_reason | string | Grund fuer Flush (z.B. "count threshold 10 reached") |

```json
{
  "items": [{"id": 1}, {"id": 2}, {"id": 3}],
  "flushed": 3,
  "waiting": false,
  "queue_name": "invoice_queue",
  "flush_reason": "count threshold 3 reached"
}
```

**Hinweis:** Items werden in der `queue_buffer` DB-Tabelle persistiert. Flush kann count-basiert (`collect_until`) oder zeitbasiert (`collect_for`) ausgeloest werden.
