# Plan: Update Tips, Help Topics, and Docs for DB-Only Pipeline Persistence

## Scope

This plan covers the persistence-related guidance that is now stale after the move to normalized DB-backed pipeline storage:

- canonical runtime persistence is DB rows: `pipeline`, `pipeline_step`, `pipeline_credential`, `pipeline_input`
- `yaml_content` is still written as a rollback/export compatibility mirror
- pipeline YAML files on disk are no longer the normal persistence layer
- `BRIX_STEP_SOURCE` controls read mode: `db`, `yaml`, `dual`

I reviewed:

- all DB tips via `BrixDB.tip_list()`
- all DB help topics via `BrixDB.help_topics_list()`
- [src/brix/mcp_handlers/help.py](/root/docker/brix/src/brix/mcp_handlers/help.py)
- [CLAUDE.md](/root/docker/brix/CLAUDE.md)

Only the items below need persistence-related changes. The other tips/help topics do not mention pipeline YAML, disk persistence, or pipeline directories in a way that conflicts with the DB-only model.

## Tips To Update

### 1. Tip: `KERN-REGEL`

Change type: DB row update

Current text:

```text
IMMER Brix MCP-Tools nutzen. KEINE Workarounds. KEINE manuellen Dateien.
KEIN docker exec. KEIN YAML schreiben. KEIN Container rebuild.
KEIN Bash(cat ~/.brix/...)       → nutze get_run_log / get_run_status
KEIN Bash(python3 -c ...)        → nutze create_helper
KEIN Bash(rm -f ...)             → nutze brix__delete_run / brix clean
```

New text:

```text
IMMER Brix MCP-Tools nutzen. KEINE Workarounds. KEINE manuellen Pipeline-Dateien.
Pipelines leben in brix.db als normale DB-Zeilen (`pipeline`, `pipeline_step`,
`pipeline_credential`, `pipeline_input`).
`yaml_content` ist nur Backup fuer Rollback/Export-Kompatibilitaet.
KEIN docker exec. KEIN YAML fuer normale CRUD. KEIN Container rebuild.
KEIN Bash(cat ~/.brix/...)       → nutze get_run_log / get_run_status
KEIN Bash(python3 -c ...)        → nutze create_helper
KEIN Bash(rm -f ...)             → nutze brix__delete_run / brix clean
```

Reason:

- The current text says what not to do, but does not explain the new source of truth.
- This tip is the highest-value place to state that pipeline persistence is now DB-native.

### 2. Tip: `TOP-5 ANTI-PATTERNS`

Change type: DB row update

Current text:

```text
delete_pipeline + create_pipeline  →  update_step / update_pipeline / add_step
YAML manuell schreiben             →  brix__create_pipeline mit steps inline
brix run via Bash                  →  brix__run_pipeline
base64 in foreach-Loops            →  Dateipfade als Strings übergeben
concurrency: '{{ input.n }}'       →  concurrency muss int sein (kein Jinja2!)
```

New text:

```text
delete_pipeline + create_pipeline  →  update_step / update_pipeline / add_step
Pipeline-YAML als Persistence sehen →  falsch: normale CRUD arbeitet auf DB-Zeilen
YAML manuell schreiben             →  nur noch fuer Import/Export/Restore-Kompatibilitaet
brix run via Bash                  →  brix__run_pipeline
base64 in foreach-Loops            →  Dateipfade als Strings uebergeben
concurrency: '{{ input.n }}'       →  concurrency muss int sein (kein Jinja2!)
```

Reason:

- The current line about YAML is directionally correct, but it still frames YAML as an authoring concern, not as a deprecated persistence path.
- The anti-pattern should explicitly reject “pipeline files are the source of truth”.

## Help Topics To Update

### 3. Help Topic: `anti-patterns`

Change type: DB row update

Current text:

```text
## 1. YAML manuell schreiben

  ❌ NIEMALS: Datei in pipelines/*.yaml direkt bearbeiten
  ✅ STATTDESSEN: brix__create_pipeline mit steps inline

  Warum: Volume-gemountet bedeutet Änderungen gehen verloren; Validierung fehlt.
...
## 7. Container rebuilden nach Pipeline-Änderungen

  ❌ NIEMALS: docker compose build nach pipeline/helper Änderungen
  ✅ STATTDESSEN: Direkt testen — Volume-Mount macht Rebuild überflüssig

  Warum: Build dauert Minuten; pipelines/ und helpers/ sind live gemountet.
...
  ❌ NIEMALS: Bash("cat ~/.brix/pipelines/<name>.yaml")
  ✅ STATTDESSEN: brix__get_pipeline(name=...)
...
  ❌ NIEMALS: Bash("ls ~/.brix/pipelines/")
  ✅ STATTDESSEN: brix__list_pipelines()
```

New text:

```text
## 1. Pipeline-YAML als Persistence behandeln

  ❌ NIEMALS: Pipeline-Dateien als normale Quelle der Wahrheit ansehen
  ❌ NIEMALS: Datei in pipelines/*.yaml direkt bearbeiten
  ✅ STATTDESSEN: create_pipeline / update_pipeline / add_step / update_step / remove_step

  Warum: Normale Pipeline-CRUD schreibt in brix.db in die Tabellen
  `pipeline`, `pipeline_step`, `pipeline_credential`, `pipeline_input`.
  `yaml_content` ist nur Mirror fuer Rollback/Export-Kompatibilitaet.
...
## 7. Container rebuilden nach Pipeline-Änderungen

  ❌ NIEMALS: docker compose build nach Pipeline-CRUD
  ✅ STATTDESSEN: Direkt testen — Persistenz ist DB-basiert

  Warum: Pipeline-Änderungen brauchen keinen Dateirewrite und keinen Rebuild.
  Helpers koennen weiter als Code-Artefakte relevant sein; Pipeline-Definitionen nicht.
...
  ❌ NIEMALS: Bash("cat ~/.brix/pipelines/<name>.yaml")
  ✅ STATTDESSEN: brix__get_pipeline(pipeline_id=...)
...
  ❌ NIEMALS: Bash("ls ~/.brix/pipelines/")
  ✅ STATTDESSEN: brix__list_pipelines()
  Warum: Listen/Details kommen aus der DB, nicht aus Verzeichnis-Scans.
```

Reason:

- This topic has the strongest remaining “pipeline files / mounted directory” language.
- It should explain DB rows explicitly instead of only warning against manual YAML edits.

### 4. Help Topic: `quick-start`

Change type: DB row update

Current text:

```text
Wichtigste Regeln:
  • KEIN YAML manuell schreiben → create_pipeline mit steps inline
  • KEIN brix run via Bash → brix__run_pipeline
  • KEIN Container-Rebuild → pipelines/ ist Volume-gemountet
  • Host-Pfade: /root/... → im Container /host/root/...
```

New text:

```text
Wichtigste Regeln:
  • Pipelines sind DB-only im Normalbetrieb: create/update/add/remove arbeiten auf DB-Zeilen
  • KEIN YAML manuell schreiben → YAML ist nur Import/Export/Restore-Kompatibilitaet
  • KEIN brix run via Bash fuer normale MCP-Arbeit → brix__run_pipeline
  • KEIN Container-Rebuild nach Pipeline-CRUD → Persistenz liegt in brix.db
  • Host-Pfade: /root/... → im Container /host/root/...
```

Reason:

- The current quick-start still explains “no rebuild” using `pipelines/` volume-mount language.
- That should be replaced with the new storage model, not the old deployment model.

### 5. Help Topic: `credentials`

Change type: DB row update

Current text:

```text
2. In Pipeline referenzieren:

  {
    "id": "call_llm",
    "type": "python",
    "config": {
      "code": "import openai; openai.api_key = credential_value"
    },
    "credentials": {"credential_value": "OPENAI_KEY"}
  }

  Oder in Jinja2:
  {{ credentials.OPENAI_KEY }}
```

New text:

```text
2. In Pipeline referenzieren:

  Pipeline-Credentials werden in `pipeline_credential` gespeichert und als
  `{{ credentials.NAME }}` im Kontext aufgeloest.

  Step-lokale Credentials sind ebenfalls moeglich:

  {
    "id": "call_llm",
    "type": "script.python",
    "credentials": {"credential_value": "OPENAI_KEY"},
    "params": {"prompt": "{{ input.prompt }}"}
  }

  Zur Laufzeit werden Step-Credentials nur fuer diesen Step ueber
  die Pipeline-Credentials gelegt.

  Oder in Jinja2:
  {{ credentials.OPENAI_KEY }}
```

Additional text to add:

```text
## Persistenz

  Pipeline-weite Credential-Definitionen leben in `pipeline_credential`.
  `yaml_content` kann diese Definitionen weiterhin spiegeln, ist aber nicht
  die normale Source of Truth.
```

Reason:

- This topic is currently incomplete for the new model.
- It should mention `pipeline_credential` and step-level credential overlay behavior implemented in [src/brix/engine.py](/root/docker/brix/src/brix/engine.py#L448).

## Code Changes

### 6. `src/brix/mcp_handlers/help.py`

Change type: code change

Current text:

```python
# List saved pipelines (from all search paths, respecting current PIPELINE_DIR)
_tips_store = PipelineStore(pipelines_dir=_pipeline_dir())
...
"## GESPEICHERTE PIPELINES",
```

New text:

```python
# List pipelines from DB-backed storage
_tips_store = PipelineStore()
...
"## PIPELINES (DB)",
```

Optional stronger change:

- Replace `PipelineStore.list_all()` here with direct `BrixDB().list_pipelines()` plus optional per-pipeline step count derived from `pipeline_step`.

Reason:

- The current comment is stale.
- Passing `_pipeline_dir()` keeps the old mental model alive even though `list_all()` is already DB-only.
- The output heading should reinforce DB-native storage instead of sounding like filesystem persistence.

### 7. `CLAUDE.md`

Change type: code/doc change

Current text:

```text
- **PIPELINES SIND DB-ONLY** -> YAML ist nur Export/Import, normale CRUD nur ueber MCP-Tools
...
brix run <pipeline.yaml> -p key=value
brix validate <pipeline.yaml>
brix run --dry-run <pipeline.yaml>
...
loader.py            # Pipeline/step loading from DB, YAML nur fuer Import/Compat
...
pipeline_store.py    # Pipeline storage layer
...
- YAML ist nur fuer Bundle-Import/Export oder Legacy-Kompatibilitaet
```

New text:

```text
- **PIPELINES SIND DB-ONLY** -> Source of Truth sind `pipeline`, `pipeline_step`,
  `pipeline_credential`, `pipeline_input`
- **`yaml_content` IST MIRROR/BACKUP** -> nur fuer Rollback, Export/Import und
  Kompatibilitaet
- **BRIX_STEP_SOURCE** -> `db` standard, `dual` fuer Paritaetschecks,
  `yaml` nur fuer Legacy/Debugging

CLI Referenz:
- Die file-path CLI (`brix run <pipeline.yaml>`, `brix validate <pipeline.yaml>`) ist
  ein Legacy/authoring Pfad fuer YAML-Loader-Workflows.
- Fuer normale Brix-Arbeit: MCP-Tools gegen DB-persistierte Pipelines nutzen.
```

Also update the architecture wording:

```text
pipeline_store.py    # DB-backed pipeline repository, yaml_content dual-write mirror
loader.py            # YAML loader + DB-row reassembly into Pipeline models
```

Reason:

- `CLAUDE.md` says DB-only in one place but still documents YAML-path CLI as if it were the normal workflow.
- The storage toggle and normalized tables are missing entirely.

## Missing Items To Add

### 8. Missing Tip: DB-only pipeline persistence

Add as new DB tip row.

Suggested title:

```text
DB-ONLY PIPELINE PERSISTENZ
```

Suggested content:

```text
Pipeline-CRUD arbeitet auf DB-Zeilen, nicht auf Pipeline-Dateien.
Source of Truth:
  - pipeline
  - pipeline_step
  - pipeline_credential
  - pipeline_input
`yaml_content` bleibt als Backup/Mirror fuer Rollback und Export erhalten.
Wenn du Pipeline-Inhalt sehen oder aendern willst:
  - get_pipeline / list_pipelines
  - update_pipeline / add_step / update_step / remove_step
```

Change type: DB row insert

### 9. Missing Help Topic: `db-persistence` or `pipeline-storage`

Add as new help topic row.

Suggested content:

```text
=== DB-Only Pipeline Persistence ===

## Source of Truth

Pipeline-Metadaten: `pipeline`
Steps: `pipeline_step`
Credentials: `pipeline_credential`
Input-Schema: `pipeline_input`

## yaml_content

`yaml_content` bleibt als dual-write Mirror fuer Export, Rollback und
Kompatibilitaet erhalten. Normale CRUD und Runtime laden jedoch aus DB-Zeilen.

## Read Mode Toggle

`BRIX_STEP_SOURCE=db`   → normale Betriebsart
`BRIX_STEP_SOURCE=dual` → DB lesen und gegen `yaml_content` validieren
`BRIX_STEP_SOURCE=yaml` → Legacy-/Debug-Pfad

## Operational Consequences

- keine normalen Pipeline-Dateien als Source of Truth
- kein Verzeichnis-Scan fuer Standard-Listing
- Step-Aenderungen sind gezielte Row-Updates statt Whole-Document-Rewrite
```

Change type: DB row insert

### 10. Missing Info: `BRIX_STEP_SOURCE`

Add in three places:

- new help topic `db-persistence` or `pipeline-storage`
- `CLAUDE.md` under the DB-only rules
- optional one-line mention in `get_tips`

Suggested short text:

```text
BRIX_STEP_SOURCE:
  db   = DB rows sind aktiv
  dual = DB rows + Vergleich mit yaml_content
  yaml = Legacy-Leseweg aus yaml_content
```

Change type: DB row update plus code/doc change

### 11. Missing Info: step-level credentials

Add in:

- help topic `credentials`
- optional anti-patterns note: do not pass secrets via input, use pipeline-level or step-level credentials

Suggested text:

```text
Step-lokale Credentials ueberschreiben den `credentials`-Kontext nur fuer die
Laufzeit dieses Steps. Das ist sinnvoll, wenn ein einzelner Step einen anderen
Token oder Alias braucht als der Rest der Pipeline.
```

Change type: DB row update

## Recommended Execution Order

1. Update DB tips:
   `KERN-REGEL`, `TOP-5 ANTI-PATTERNS`, add `DB-ONLY PIPELINE PERSISTENZ`
2. Update DB help topics:
   `anti-patterns`, `quick-start`, `credentials`
3. Add new help topic:
   `db-persistence` or `pipeline-storage`
4. Update [src/brix/mcp_handlers/help.py](/root/docker/brix/src/brix/mcp_handlers/help.py):
   remove stale search-path wording and rename the pipeline section header
5. Update [CLAUDE.md](/root/docker/brix/CLAUDE.md):
   normalize persistence language, add `BRIX_STEP_SOURCE`, demote YAML-path CLI to legacy/compat wording

## Notes

- I did not find other tip rows that require pipeline-persistence changes.
- I did not find other help topics that explicitly reference pipeline YAML or pipeline directories in a way that conflicts with DB-only persistence.
- There is separate broader doc drift around legacy step type names and older examples, but that is outside this persistence-only pass.
