

# Brix Frontend — UX-Architektur und Umsetzungsplan

Bevor ich das Design entwerfe, schaue ich mir die bestehende Codebase an, um die tatsaechlichen Datenstrukturen und API-Moeglichkeiten zu verstehen.
Jetzt habe ich genug Kontext. Hier ist mein vollstaendiger Entwurf.

---

# Brix Frontend — UX-Architektur

## 1. Tech-Stack-Empfehlung

| Schicht | Empfehlung | Begruendung |
|---------|-----------|-------------|
| **Framework** | Next.js 15 (App Router) | File-based Routing, SSR fuer schnelle Erstladung, API Routes als BFF-Proxy |
| **UI-Library** | shadcn/ui + Tailwind CSS v4 | Composable, Theming, Dark Mode, Copy-Paste statt Dependency |
| **Flow-Viewer** | React Flow (xyflow) | Bestes Open-Source-Tool fuer Node-Graphen, gute Layouts, Custom Nodes |
| **Datentabellen** | TanStack Table v8 | Sortieren, Filtern, Virtualisierung — fuer Run-History und Brick-Katalog |
| **State** | Zustand + TanStack Query | Zustand fuer UI-State, TanStack Query fuer Server-State und Caching |
| **JSON-Viewer** | react-json-view-lite | Fuer Step-Inspector Input/Output Darstellung |
| **Charts** | Recharts | Leichtgewichtig, fuer Health-Dashboard Metriken |
| **Deployment** | Docker Container neben brix-api | Im gleichen shared-network, spricht direkt mit brix-api:8090 |

**Warum Next.js statt Vite-SPA:** Brix hat bereits eine REST-API (Starlette auf Port 8090). Next.js API Routes koennen als BFF (Backend-for-Frontend) agieren und die existierende REST-API plus direkte SQLite-Reads buendeln, ohne dass die Brix-Python-API um 20 neue Endpoints erweitert werden muss. Alternativ: Reine Vite-SPA, die direkt gegen brix-api:8090 spricht — dann muessen aber mehr Endpoints in der Python-API ergaenzt werden.

**Meine Empfehlung: Next.js als BFF**, weil die existierende API nur 7 Endpoints hat, das Frontend aber Daten aus ~10 DB-Tabellen braucht.

---

## 2. Informationsarchitektur

### Hauptnavigation (Sidebar, immer sichtbar)

```
+------------------------------------------+
|  BRIX                          [v4.4.3]  |
|------------------------------------------|
|  > Dashboard                             |
|  > Pipelines                             |
|  > Runs                                  |
|  > Bricks                                |
|  > Triggers                              |
|  > Variables                             |
|  > Connections                           |
|  > System                                |
+------------------------------------------+
```

**7 Top-Level-Seiten.** Keine verschachtelten Menues. Jede Seite ist ein Einstiegspunkt, der in Details drill-down kann.

### Drill-Down-Pfade

```
Dashboard ──────────────────────────────────────────────
  └─ Klick auf Pipeline-Karte → /pipelines/{name}
  └─ Klick auf letzten Run    → /runs/{run_id}
  └─ Klick auf Alert          → /system

Pipelines ──────────────────────────────────────────────
  /pipelines                        (Liste aller Pipelines)
  /pipelines/{name}                 (Detail: Flow-Viewer + Runs + Config)
  /pipelines/{name}/runs/{run_id}   (Run-Detail mit Step-Inspector)

Runs ───────────────────────────────────────────────────
  /runs                             (Alle Runs, uebergreifend)
  /runs/{run_id}                    (Run-Detail, identisch wie oben)
  /runs/{run_id}/steps/{step_id}    (Step-Inspector Deep-Link)

Bricks ─────────────────────────────────────────────────
  /bricks                           (Katalog mit Namespace-Gruppierung)
  /bricks/{name}                    (Detail: Schema, Beispiele, genutzt-von)

Triggers ───────────────────────────────────────────────
  /triggers                         (Alle Trigger + Gruppen)
  /triggers/{id}                    (Detail + letzte Fires)

Variables ──────────────────────────────────────────────
  /variables                        (KV-Store + Pipeline-Variables)

Connections ────────────────────────────────────────────
  /connections                      (MCP Server + Connectors)

System ─────────────────────────────────────────────────
  /system                           (Health, Alerts, App-Log, DB-Stats)
```

---

## 3. Schluessel-Screens im Detail

### 3.1 Dashboard (`/`)

**Zweck:** Auf einen Blick sehen ob alles laeuft.

```
+---------------------------------------------------------------+
|  DASHBOARD                                     [Last 24h v]   |
|---------------------------------------------------------------|
|                                                               |
|  +------------+  +------------+  +------------+  +----------+ |
|  | 147        |  | 139        |  | 8          |  | 3        | |
|  | Total Runs |  | Succeeded  |  | Failed     |  | Running  | |
|  +------------+  +------------+  +------------+  +----------+ |
|                                                               |
|  [===== Runs Timeline (Sparkline 24h) =====]                 |
|                                                               |
|  AKTIVE RUNS                                                  |
|  +-----------------------------------------------------------+|
|  | buddy-intake-outlook  run-a3f8..  Step 4/7  ██████░░  2m ||
|  | cody-cost-alert       run-7e12..  Step 2/3  ████░░░░  8s ||
|  +-----------------------------------------------------------+|
|                                                               |
|  LETZTE FEHLGESCHLAGENE RUNS                                  |
|  +-----------------------------------------------------------+|
|  | buddy-fetch-batch  run-9c21..  13:42  "HTTPError 429"    ||
|  | convert-pdf        run-b77e..  12:01  "Timeout after 5m" ||
|  +-----------------------------------------------------------+|
|                                                               |
|  TOP PIPELINES (letzte 7 Tage)                                |
|  +-----------------------------------------------------------+|
|  | Pipeline             | Runs | Erfolg | Avg Dauer | Trend  ||
|  |----------------------|------|--------|-----------|--------||
|  | buddy-intake-outlook | 84   | 98%    | 4m 12s    | ↑      ||
|  | buddy-classify       | 67   | 100%   | 1m 03s    | →      ||
|  | download-attachments | 31   | 87%    | 2m 45s    | ↓      ||
|  +-----------------------------------------------------------+|
|                                                               |
|  ALERTS                                                       |
|  [!] buddy-fetch-batch: 3 Failures in letzter Stunde         |
|  [i] System: DB Size 487 MB / 500 MB Limit                   |
+---------------------------------------------------------------+
```

**Datenquellen:** `runs`-Tabelle (Aggregationen), `alert_rules`/`alert_history`, live `run.json` fuer aktive Runs.

### 3.2 Pipeline-Liste (`/pipelines`)

```
+---------------------------------------------------------------+
|  PIPELINES                          [Search...] [Filter v]    |
|---------------------------------------------------------------|
|  Showing 47 pipelines               Sort: Last Run v          |
|                                                               |
|  +-----------------------------------------------------------+|
|  | buddy-intake-outlook                              [ACTIVE]||
|  | "Intake Outlook emails via M365, classify, file"          ||
|  | v2.1.0 | 7 Steps | Last run: 2m ago (OK)                 ||
|  | Triggers: mail-trigger-outlook (every 5m)                 ||
|  +-----------------------------------------------------------+|
|  | buddy-classify-and-move                                   ||
|  | "Classify OneDrive documents and move to folders"         ||
|  | v1.4.0 | 12 Steps | Last run: 14m ago (OK)               ||
|  +-----------------------------------------------------------+|
|  | download-attachments                                      ||
|  | "Download M365 mail attachments with OData filter"        ||
|  | v1.0.0 | 4 Steps | Last run: 2h ago (FAILED)             ||
|  +-----------------------------------------------------------+|
```

**Filter:** All / Active (hat Trigger) / Templates / Test-Pipelines. **Sort:** Name, Last Run, Run Count, Failure Rate.

### 3.3 Pipeline-Detail (`/pipelines/{name}`)

Drei Tabs: **Flow** | **Runs** | **Config**

#### Tab: Flow (Pipeline-Viewer — das Herzsueck)

```
+---------------------------------------------------------------+
|  buddy-intake-outlook                 v2.1.0    [Run Now]     |
|  "Intake Outlook emails via M365, classify, file"             |
|---------------------------------------------------------------|
|  [Flow]  [Runs]  [Config]                                     |
|---------------------------------------------------------------|
|                                                               |
|   ┌──────────┐    ┌──────────────┐    ┌────────────────────┐  |
|   │ fetch-   │    │ filter-new   │    │ foreach: process   │  |
|   │ emails   │───>│              │───>│ ┌──────────────┐   │  |
|   │ [mcp]    │    │ [filter]     │    │ │ classify [py]│   │  |
|   └──────────┘    └──────────────┘    │ │      ↓       │   │  |
|                                       │ │ move [mcp]   │   │  |
|                                       │ └──────────────┘   │  |
|                                       │ concurrency: 5     │  |
|                                       └────────────────────┘  |
|                         ↓                                     |
|   ┌──────────┐    ┌──────────────┐                            |
|   │ notify   │<───│ summary      │                            |
|   │ [notify] │    │ [set]        │                            |
|   └──────────┘    └──────────────┘                            |
|                                                               |
|  [Klick auf Node oeffnet Step-Detail-Panel rechts]            |
+---------------------------------------------------------------+
```

**Step-Detail-Panel (Slide-Over von rechts, 400px breit):**

```
+-------------------------------+
|  Step: classify               |
|  Type: python                 |
|  Helper: buddy_classify       |
|-------------------------------|
|  PARAMS                       |
|  model: "{{ input.model }}"   |
|  categories:                  |
|    - Rechnung                 |
|    - Vertrag                  |
|    - Versicherung             |
|-------------------------------|
|  FLOW                         |
|  foreach: "{{ steps.filter..  |
|  parallel: true               |
|  concurrency: 5               |
|  on_error: continue           |
|-------------------------------|
|  SCHEMA                       |
|  input:  {text: string}       |
|  output: {category: string,   |
|           confidence: float}  |
+-------------------------------+
```

#### Tab: Runs

Tabelle aller Runs dieser Pipeline. Klick auf Run oeffnet Run-Detail.

#### Tab: Config

Raw YAML der Pipeline (read-only, Syntax-Highlighted), Input-Parameter-Schema, Credentials (maskiert), Error-Handling-Config.

### 3.4 Run-Detail (`/runs/{run_id}`)

**Das n8n-Execution-Aequivalent.** Zeigt den Flow-Graphen MIT Run-Daten-Overlay.

```
+---------------------------------------------------------------+
|  Run run-a3f8c7e1   buddy-intake-outlook                      |
|  Started: 14:32:01  Duration: 4m 12s  Status: [SUCCESS]       |
|---------------------------------------------------------------|
|                                                               |
|   ┌──────────┐    ┌──────────────┐    ┌────────────────────┐  |
|   │ fetch-   │    │ filter-new   │    │ foreach: process   │  |
|   │ emails   │───>│              │───>│  42/42 items OK    │  |
|   │ ✓ 3.2s   │    │ ✓ 0.1s       │    │ ✓ 3m 48s           │  |
|   │ 67 items │    │ 42 passed    │    │ 2 retried          │  |
|   └──────────┘    └──────────────┘    └────────────────────┘  |
|                         ↓                                     |
|   ┌──────────┐    ┌──────────────┐                            |
|   │ notify   │<───│ summary      │                            |
|   │ ✓ 0.4s   │    │ ✓ 0.0s       │                            |
|   └──────────┘    └──────────────┘                            |
|                                                               |
|  [Klick auf Node → Step-Inspector]                            |
+---------------------------------------------------------------+
```

**Farb-Kodierung der Nodes:**
- Gruen = success
- Rot = failed
- Grau = skipped (when-Condition false)
- Blau-pulsierend = running
- Gelb = retried/warn

### 3.5 Step-Inspector (`/runs/{run_id}/steps/{step_id}`)

Panel rechts oder als Full-Page. Das ist das Power-Tool fuer Debugging.

```
+---------------------------------------------------------------+
|  Step: classify               Status: ✓ Success   Duration: 2.1s
|---------------------------------------------------------------|
|  [Input]  [Output]  [Params]  [Stderr]  [Context]            |
|---------------------------------------------------------------|
|                                                               |
|  INPUT (was der Step empfangen hat):                          |
|  {                                                            |
|    "text": "Rechnung Nr. 2024-0847 ...",                      |
|    "filename": "rechnung_vodafone.pdf",                       |
|    "source": "inbox"                                          |
|  }                                                            |
|                                                               |
|  OUTPUT (was der Step zurueckgegeben hat):                    |
|  {                                                            |
|    "category": "Rechnung",                                    |
|    "confidence": 0.97,                                        |
|    "subcategory": "Telekommunikation"                         |
|  }                                                            |
|                                                               |
|  [Bei foreach: Item-Selector oben — Dropdown mit 42 Items]   |
|  Item 1/42: vodafone_2024.pdf  [<] [>]                       |
+---------------------------------------------------------------+
```

**Tabs im Step-Inspector:**
- **Input** — JSON-Viewer mit dem tatsaechlichen Input-Wert
- **Output** — JSON-Viewer mit dem tatsaechlichen Output-Wert
- **Params** — Die gerenderten (aufgeloesten) Parameter nach Jinja2-Rendering
- **Stderr** — Helper-Script stderr-Output (Logs, Warnings)
- **Context** — Snapshot des Pipeline-Kontexts zu diesem Zeitpunkt

**Datenquelle:** `step_outputs`-Tabelle (output_json, rendered_params_json, stderr_text, context_json). Fuer Steps ohne `persist_output: true` zeigt der Inspector "No data persisted — enable persist_output on this step".

### 3.6 Brick-Katalog (`/bricks`)

```
+---------------------------------------------------------------+
|  BRICKS                             [Search...]               |
|---------------------------------------------------------------|
|  51 Bricks in 8 Namespaces                                   |
|                                                               |
|  FLOW (12)                                                    |
|  +-----------------------------------------------------------+|
|  | filter        | Filter items by Jinja2 condition    [→]   ||
|  | transform     | Transform data with Jinja2 mapping  [→]   ||
|  | set           | Set context variables                [→]   ||
|  | choose        | If/else branching                    [→]   ||
|  | switch        | Multi-way branching                  [→]   ||
|  | repeat        | Loop until/while condition           [→]   ||
|  | parallel      | Run sub-steps concurrently           [→]   ||
|  | ...                                                       ||
|  +-----------------------------------------------------------+|
|                                                               |
|  SCRIPT (2)                                                   |
|  +-----------------------------------------------------------+|
|  | python        | Run Python script/helper             [→]   ||
|  | cli           | Run shell command                    [→]   ||
|  +-----------------------------------------------------------+|
|                                                               |
|  MCP (1)        HTTP (1)        DB (2)                        |
|  ACTION (3)     EXTRACT (1)     SOURCE (1)                    |
```

#### Brick-Detail (`/bricks/{name}`)

```
+---------------------------------------------------------------+
|  Brick: flow.filter                                           |
|  Namespace: flow    Runner: filter    Category: flow-control  |
|---------------------------------------------------------------|
|  DESCRIPTION                                                  |
|  Filter a list of items using a Jinja2 boolean expression.    |
|  Items where the expression evaluates to true are kept.       |
|                                                               |
|  WHEN TO USE                                                  |
|  When you need to reduce a list based on conditions.          |
|                                                               |
|  WHEN NOT TO USE                                              |
|  When you need to transform data (use flow.transform).        |
|                                                               |
|  CONFIG SCHEMA                                                |
|  +-----------------------------------------------------------+|
|  | Parameter   | Type   | Required | Default | Description   ||
|  |-------------|--------|----------|---------|---------------||
|  | condition   | string | yes      | —       | Jinja2 expr   ||
|  | input_field | string | no       | "items" | Source field   ||
|  +-----------------------------------------------------------+|
|                                                               |
|  EXAMPLES                                                     |
|  +-----------------------------------------------------------+|
|  | Goal: "Filter emails older than 7 days"                   ||
|  | Config:                                                   ||
|  |   condition: "{{ item.age_days > 7 }}"                    ||
|  +-----------------------------------------------------------+|
|                                                               |
|  USED BY (3 Pipelines)                                        |
|  - buddy-intake-outlook (Step: filter-new)                    |
|  - buddy-classify-and-move (Step: filter-unclassified)        |
|  - download-attachments (Step: filter-pdfs)                   |
+---------------------------------------------------------------+
```

### 3.7 Trigger-Dashboard (`/triggers`)

```
+---------------------------------------------------------------+
|  TRIGGERS                                                     |
|---------------------------------------------------------------|
|  TRIGGER GROUPS                                               |
|  +-----------------------------------------------------------+|
|  | buddy-triggers   | 3 triggers | RUNNING | Started 2h ago ||
|  | cody-monitors    | 2 triggers | STOPPED |                ||
|  +-----------------------------------------------------------+|
|                                                               |
|  ALL TRIGGERS                                                 |
|  +-----------------------------------------------------------+|
|  | ID                 | Type  | Pipeline          | Interval ||
|  |--------------------|-------|-------------------|----------||
|  | mail-outlook       | mail  | buddy-intake-outl | 5m   ✓  ||
|  | file-onedrive-scan | file  | buddy-onedrive-sc | 15m  ✓  ||
|  | pipeline-chain     | p.done| buddy-classify    | —    ✓  ||
|  | cost-alert         | cron  | cody-cost-alert   | 1h   ✗  ||
|  +-----------------------------------------------------------+|
|                                                               |
|  Klick auf Trigger zeigt:                                     |
|  - Config (filter, dedupe_key, forward_input)                 |
|  - Letzte 20 Fires mit Timestamp + resulting run_id           |
|  - Debounce-Status (wenn konfiguriert)                        |
+---------------------------------------------------------------+
```

### 3.8 Variable-Editor (`/variables`)

Zwei Sections: **Pipeline Variables** (Jinja2 `{{ var.X }}`) und **Persistent Store** (KV-Store fuer Steps mit `persist: true`).

```
+---------------------------------------------------------------+
|  VARIABLES                                                    |
|---------------------------------------------------------------|
|  PIPELINE VARIABLES ({{ var.X }})                 [+ Add]     |
|  +-----------------------------------------------------------+|
|  | Key                | Value              | Updated          ||
|  |--------------------|--------------------|-----------------||
|  | default_model      | gpt-4o-mini        | 2h ago          ||
|  | batch_size         | 50                 | 3d ago          ||
|  | outlook_folder     | INBOX              | 1w ago          ||
|  +-----------------------------------------------------------+|
|                                                               |
|  PERSISTENT STORE (step persist: true)            [+ Add]     |
|  +-----------------------------------------------------------+|
|  | Key                | Value (preview)     | Updated         ||
|  |--------------------|--------------------|-----------------||
|  | last_sync_cursor   | "AAMkADQ3..."      | 5m ago          ||
|  | processed_ids      | [1847 items]        | 2m ago          ||
|  +-----------------------------------------------------------+|
|                                                               |
|  Klick auf Zeile oeffnet Edit-Dialog mit JSON-Editor          |
+---------------------------------------------------------------+
```

### 3.9 System/Health (`/system`)

```
+---------------------------------------------------------------+
|  SYSTEM                                                       |
|---------------------------------------------------------------|
|  [Health]  [Alerts]  [App Log]  [MCP Servers]                 |
|---------------------------------------------------------------|
|                                                               |
|  HEALTH                                                       |
|  +-----------------------------------------------------------+|
|  | Component      | Status  | Details                        ||
|  |----------------|---------|--------------------------------||
|  | Brix Engine    | ✓ OK    | v4.4.3, uptime 14d 3h         ||
|  | SQLite DB      | ✓ OK    | 487 MB, 12,847 runs           ||
|  | MCP Pool       | ✓ OK    | 4/4 servers connected          ||
|  | Trigger Svc    | ✓ OK    | 3 active, 2 groups running     ||
|  | Scheduler      | ⚠ WARN  | 1 missed schedule              ||
|  +-----------------------------------------------------------+|
|                                                               |
|  MCP SERVERS                                                  |
|  +-----------------------------------------------------------+|
|  | Server         | Status  | Tools | Last Health             ||
|  |----------------|---------|-------|-------------------------||
|  | m365           | ✓ OK    | 47    | 30s ago                 ||
|  | n8n-pilot      | ✓ OK    | 62    | 30s ago                 ||
|  | cody           | ✓ OK    | 89    | 30s ago                 ||
|  | twin           | ✗ DOWN  | —     | 5m ago (timeout)        ||
|  +-----------------------------------------------------------+|
|                                                               |
|  APP LOG (letzte 50 Eintraege)                                |
|  [ERROR] [WARN] [INFO] Filter                                |
|  14:32:01 ERROR triggers  mail-trigger: IMAP connection refused|
|  14:31:45 INFO  engine    Run run-a3f8 completed (4m12s)      |
|  14:27:33 WARN  mcp_pool  twin server: health check timeout   |
+---------------------------------------------------------------+
```

---

## 4. Pipeline-Viewer — Detailentwurf

### Node-Typen als React Flow Custom Nodes

Jeder Step-Typ bekommt ein eigenes visuelles Node-Design:

| Step-Typ | Node-Form | Farbe | Icon |
|-----------|----------|-------|------|
| python/cli | Rechteck | Slate/Grau | `</>` Code-Icon |
| http | Rechteck | Blue | Globe-Icon |
| mcp | Rechteck | Purple | Plug-Icon |
| filter | Raute (Diamond) | Amber | Funnel-Icon |
| transform | Rechteck mit abgerundeten Ecken | Teal | Arrows-Icon |
| set | Kleines Quadrat | Gray | Variable-Icon |
| choose | Raute mit Branches | Orange | GitBranch-Icon |
| switch | Raute mit N Branches | Orange | GitFork-Icon |
| notify | Rechteck | Green | Bell-Icon |
| pipeline (sub) | Doppelter Rahmen | Indigo | Layers-Icon |
| foreach-Container | Gestrichelter Rahmen um Sub-Nodes | — | Loop-Icon |
| parallel-Container | Gestrichelter Rahmen, horizontal | — | Columns-Icon |

### Foreach/Parallel als Group-Nodes

```
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
  foreach: {{ steps.fetch... }}
│ concurrency: 5    42 items   │
  ┌──────────┐   ┌──────────┐
│ │ classify │──>│ move     │  │
  │ [python] │   │ [mcp]    │
│ └──────────┘   └──────────┘  │
└ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
```

Foreach-Steps die `foreach` + Sub-Steps in `sequence` oder implizit haben, werden als **Group Node** gerendert. Die inneren Steps sind Children im React Flow Graph.

### Choose/Switch als Branch-Nodes

```
                ┌──────────────┐
           ┌───>│ branch-a     │
           │    │ when: x > 5  │
┌─────────┐│    └──────────────┘
│ choose  │├───>┌──────────────┐
│ [◇]     ││    │ branch-b     │
└─────────┘│    │ when: x <= 5 │
           │    └──────────────┘
           └───>┌──────────────┐
                │ default      │
                └──────────────┘
```

### Layout-Algorithmus

**Dagre** (via `@dagrejs/dagre`) mit `rankdir: "TB"` (top-to-bottom) als Default. Option auf `"LR"` (left-to-right) per Toggle. Steps ohne `depends_on` werden aus der `steps`-Liste sequentiell verbunden. Steps mit `depends_on` werden per DAG-Kanten verbunden.

### Run-Overlay

Im Run-Detail-View werden die gleichen Nodes gezeigt, aber mit Overlay-Daten:

- **Badge oben-rechts:** Dauer (z.B. "3.2s")
- **Badge unten:** Item-Count oder Output-Groesse
- **Border-Farbe:** Success/Failed/Skipped/Running
- **Pulsierender Rand:** Fuer aktuell laufende Steps (via SSE `/stream/{run_id}`)

---

## 5. Backend-Anbindung

### Strategie: Next.js API Routes als BFF

Die existierende brix-api hat nur 7 Endpoints. Das Frontend braucht aber:
- Brick-Katalog (aus `brick_definitions`-Tabelle)
- Trigger-Liste (aus `triggers`/`trigger_groups`-Tabelle)
- Variable-Liste (aus `variables`-Tabelle + `persistent_store`)
- Step-Outputs (aus `step_outputs`-Tabelle)
- App-Log (aus `app_log`-Tabelle)
- Pipeline-Details mit Steps (aus Pipeline-YAML oder DB)

**Option A (empfohlen):** Next.js API Routes lesen direkt die SQLite-DB (`~/.brix/brix.db`), die per Volume gemountet wird. Read-Only — alle Schreiboperationen gehen ueber die existierende REST-API oder MCP.

**Option B:** Brix-API um 15+ Endpoints erweitern. Mehr Aufwand, sauberere Architektur langfristig.

Ich empfehle **Option A fuer den Start**, weil es sofort alle Daten liefert ohne Python-Code zu aendern. Spaeter kann die REST-API schrittweise erweitert werden.

### Echtzeit-Updates

- **SSE** (`/stream/{run_id}`) existiert bereits fuer laufende Runs — direkt ans Frontend durchreichen
- **Polling** (5s Intervall) fuer Dashboard-Zahlen und Trigger-Status — TanStack Query mit `refetchInterval`
- Kein WebSocket noetig im ersten Schritt

---

## 6. Umsetzungs-Reihenfolge

### Phase 1: Grundgeruest + Pipeline-Viewer (Woche 1-2)

**Warum zuerst:** Der Pipeline-Viewer ist das Feature mit dem hoechsten Erkenntnisgewinn — Hans sieht zum ersten Mal visuell, was seine Pipelines tun.

1. Next.js Projekt aufsetzen, Docker Container, shadcn/ui, Tailwind
2. Sidebar-Navigation mit 7 Seiten (Placeholder)
3. `/pipelines` — Liste aller Pipelines (aus DB)
4. `/pipelines/{name}` — Pipeline-Detail mit React Flow Viewer
5. Node-Typen als Custom Nodes (Farben, Icons, Labels)
6. Foreach/Parallel als Group Nodes
7. Choose/Switch als Branch-Nodes
8. Step-Detail-Panel (Slide-Over rechts) bei Klick auf Node

**Ergebnis:** Hans kann alle Pipelines sehen und ihre Flows visuell erkunden.

### Phase 2: Run-History + Step-Inspector (Woche 3-4)

9. `/runs` — Run-History-Tabelle (Filter, Sort, Search)
10. `/runs/{run_id}` — Flow-Graph mit Run-Overlay (Farben, Dauer, Status)
11. Step-Inspector (Input/Output/Params/Stderr-Tabs)
12. SSE-Integration fuer Live-Runs (pulsierender Rand, Progress)
13. Foreach-Item-Navigator im Step-Inspector

**Ergebnis:** Hans kann Runs debuggen wie in n8n Executions.

### Phase 3: Dashboard + Bricks + Variables (Woche 5-6)

14. Dashboard mit KPI-Karten, aktive Runs, letzte Fehler, Top-Pipelines
15. Brick-Katalog mit Namespace-Gruppierung und Detail-Seite
16. Variable-Editor (lesen + schreiben via REST-API)
17. Persistent-Store-Viewer

**Ergebnis:** Vollstaendiges Bild des Systems.

### Phase 4: Triggers + System + Polish (Woche 7-8)

18. Trigger-Dashboard mit Gruppen, Status, letzte Fires
19. Connection/MCP-Server-Uebersicht mit Health-Status
20. System-Seite: Health, Alerts, App-Log
21. Dark Mode Toggle
22. Responsive Sidebar (Collapse auf Mobile)

**Ergebnis:** Feature-Complete fuer v1.

---

## 7. Docker-Integration

```yaml
# docker-compose.yml Ergaenzung
  brix-ui:
    build: ./frontend
    container_name: brix-ui
    ports:
      - "3000:3000"
    volumes:
      - ${HOME}/.brix:/data/.brix:ro          # SQLite DB read-only
      - ./frontend/src:/app/src                # Dev-Mode
    environment:
      - BRIX_API_URL=http://brix-api:8090
      - BRIX_DB_PATH=/data/.brix/brix.db
    networks:
      - shared-network
    depends_on:
      - brix-api
```

---

## Zusammenfassung

| Aspekt | Entscheidung |
|--------|-------------|
| **Framework** | Next.js 15 + shadcn/ui + Tailwind |
| **Flow-Viewer** | React Flow mit Custom Nodes + Dagre Layout |
| **Datenanbindung** | SQLite direkt (read) + REST-API (write) + SSE (live) |
| **Navigation** | 7 Top-Level-Seiten, Drill-Down per Klick |
| **Reihenfolge** | Pipeline-Viewer zuerst, dann Run-History, dann Rest |
| **Deployment** | Docker Container im shared-network |

Das Design bildet die gesamte Brix-Architektur ab: Pipelines, Steps, Bricks, Triggers, Variables, Connections, Health — alles was existiert wird sichtbar. Der Einstieg ist ueberall moeglich (Dashboard, Pipeline-Liste, Run-History), und von jedem Punkt kommt man per Klick in die Details.
