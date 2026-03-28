# Integration Learnings

Real-world findings from deploying and testing Brix. These document the **intent** behind each decision so users facing similar challenges understand the reasoning.

---

## 1. Host Docker Binary Mount (not install in image)

**Problem:** Brix needs `docker exec -i` to reach MCP servers running in other containers. Initial approach was installing Docker CLI inside the Brix image (`apt-get install docker.io` or downloading the static binary).

**Why it failed:**
- `docker.io` package on Debian slim installs the daemon, not the CLI client
- Downloading a static binary works but adds build time, image size, and version management overhead

**Intent:** Brix is a system integration tool, not an isolated application. It should use what the host already has — not maintain its own copy. The host already has a working Docker CLI that matches the Docker Engine version.

**Solution:**
```yaml
volumes:
  - /usr/bin/docker:/usr/bin/docker:ro  # host binary, read-only
  - /var/run/docker.sock:/var/run/docker.sock  # Docker API access
```

**For users:** If your MCP servers run as Docker containers, mount your host's Docker binary into the Brix container. This ensures version compatibility and zero maintenance overhead.

---

## 2. Container as Persistent Service (sleep infinity)

**Problem:** Initial Dockerfile used `ENTRYPOINT ["brix"]` — the container starts, brix shows help (no arguments), exits. With `restart: unless-stopped`, this creates a restart loop.

**Intent:** Brix is not a long-running server. It's a CLI tool that Claude invokes on demand. But it needs to be available instantly — starting a new container for each `brix run` adds 1-2 seconds overhead. The container should stay alive and wait for commands.

**Solution:**
```dockerfile
CMD ["sleep", "infinity"]
```

Claude invokes via `docker exec brix brix run ...`. A wrapper script at `/usr/local/bin/brix` makes this transparent.

**For users:** Don't use ENTRYPOINT for CLI tools in always-on containers. Use `sleep infinity` and invoke via `docker exec`.

---

## 3. MCP Server Registration: docker exec -i as stdio bridge

**Problem:** MCP servers run in their own containers (e.g., `m365` container with `ms-365-mcp-server`). Brix needs to communicate via the MCP stdio protocol (JSON-RPC over stdin/stdout).

**Intent:** Brix should not care whether an MCP server runs on the host or in a container. The `servers.yaml` configuration should look the same either way — just `command` + `args`.

**Solution:**
```bash
brix server add m365 \
  --command docker \
  --args exec --args -i --args m365 --args ms-365-mcp-server
```

`docker exec -i` transparently bridges stdin/stdout between Brix and the containerized MCP server. The MCP Python SDK's stdio client works identically — it doesn't know or care that the server is in another container.

**For users:** Register container-based MCP servers with `docker exec -i <container> <server-binary>` as the command. The stdio protocol flows transparently. No HTTP, no ports, no networking configuration needed.

---

## 4. Credentials: MCP Server Handles Auth Internally

**Problem:** The download-attachments pipeline was designed with `BRIX_CRED_M365_TOKEN` for HTTP download steps. But M365 uses OAuth2 with token refresh — the MCP server manages this internally.

**Intent:** Brix should not duplicate auth logic that MCP servers already handle. If the MCP server is authenticated, Brix should leverage that — not maintain separate credentials for the same service.

**Learning:** For MCP-backed integrations, the MCP server handles auth. The `credentials` section in pipeline YAML is for direct HTTP calls that bypass MCP (e.g., downloading files from URLs the MCP server returns). Not every pipeline needs credentials.

**For users:** If your MCP server handles authentication (OAuth, API keys, etc.), you don't need to configure those credentials in Brix. Only configure credentials that Brix's HTTP runner needs for direct API calls.

---

## 5. First Successful E2E: MCP Fetch (4.4s, 1 tool call)

**Result:** Brix successfully fetched real M365 emails via:
```
Claude → brix run → MCP Runner → docker exec -i m365 → ms-365-mcp-server → Microsoft Graph API
```

**Token savings validated:** This single `brix run` call replaced what would have been multiple Claude tool calls (list-mail-messages, then per-message operations). Claude sees one Bash call and one JSON result.

---

## 6. Host Filesystem Access: /host/root prefix

**Problem:** Pipeline saves attachments to `/tmp/brix-attachments/` — but that's inside the container. The user wants files at `/root/dev/markdown-tests/brix/` on the host.

**Intent:** Brix is a system tool. Its output should land where the user needs it — on the host filesystem, not hidden inside a container. The user should be able to specify any host path as output_dir.

**Solution:**
```yaml
# docker-compose.yml
volumes:
  - /root:/host/root  # host /root → container /host/root
```

Pipeline parameter uses the container path:
```bash
brix run pipeline.yaml -p output_dir=/host/root/dev/markdown-tests/brix
```

Files appear at `/root/dev/markdown-tests/brix/` on the host.

**Convention:** `/host/root/...` in Brix = `/root/...` on host. This prefix is the cost of containerization — but it's predictable and consistent.

**For users:** Mount the host directories you need into the Brix container. Use `/host/<path>` as the prefix in pipeline parameters. Files written there appear on the host immediately.

---

## 7. Jinja2 Dict Rendering Bug (v0.6.3 fix)

**Problem:** Step A outputs `{"value": [...]}`. Step B references `{{ A.output }}`. Jinja2 renders this as Python repr: `{'value': [...]}` (single quotes) — not valid JSON (`{"value": [...]}`). Every subsequent step that tries to parse this as JSON fails silently or gets garbage data.

**Intent:** Data flow between steps must be lossless. If step A produces a dict, step B must receive an identical dict — not a mangled string representation.

**Root cause:** Jinja2's default string coercion calls Python's `str()` / `repr()` on objects, which uses single quotes for dicts. The loader's `render_value` tried `json.loads` on the result — which fails for Python repr.

**Fix (v0.6.3):** Added `ast.literal_eval` as fallback in `render_value`. Python repr strings like `{'key': 'val'}` are safely parsed back to dicts. Also added `tojson` filter for explicit JSON rendering: `{{ step.output | tojson }}`.

**Impact:** This bug affected EVERY pipeline where a step output (dict or list) was referenced by a subsequent step. It was invisible in tests because test data was simple strings — only surfaced with real M365 API responses.

**For users:** If you see steps receiving empty or garbled data, this was likely the cause. Upgrade to v0.6.3+. Use `| tojson` filter when you need guaranteed JSON strings.

---

## 8. Large Payload: stdin vs argv (v0.6.3 fix)

**Problem:** Python runner passes params via `sys.argv[1]`. OS has a limit (~128KB on Linux). Real M365 mail responses easily exceed this — `Argument list too long` error.

**Intent:** Brix should handle any payload size between steps, not just small test data.

**Fix:** Python runner detects params >100KB and switches to stdin:
```python
if len(params_json) > 100_000:
    # Pass via stdin instead of argv
    proc = await asyncio.create_subprocess_exec(
        "python3", script,
        stdin=asyncio.subprocess.PIPE, ...
    )
    await proc.communicate(input=params_json.encode())
```

Helper scripts read from `sys.argv[1]` first, fall back to `sys.stdin.read()`.

**For users:** Your helper scripts should support both input methods:
```python
if len(sys.argv) > 1:
    params = json.loads(sys.argv[1])
elif not sys.stdin.isatty():
    raw = sys.stdin.read().strip()
    params = json.loads(raw) if raw else {}
```

---

## 9. Global Discovery: Claude muss Brix in JEDER Session kennen

**Problem:** Brix funktioniert — aber nur Claude-Sessions die im Brix-Repo arbeiten wissen davon. In einer Mailpilot-Session oder einem anderen Projekt hat Claude keine Ahnung dass `brix` existiert. Der User muss es manuell erwähnen.

**Intent:** Brix ist ein System-Tool, kein Projekt-Tool. Es soll von überall nutzbar sein — wie `git` oder `docker`. Jede Claude-Instanz auf diesem Server soll wissen: "Für Multi-Step-Aufgaben gibt es `brix run`."

**Root cause:** Drei Ebenen der Discovery, und nur eine war aktiv:

| Ebene | Scope | Status vorher |
|-------|-------|---------------|
| `CLAUDE.md` im Brix-Repo | Nur im Repo | Hatte Brix dokumentiert |
| `~/.claude/commands/*.md` | Alle Projekte | Skills waren da, aber Claude nutzt sie nur wenn User `/` tippt |
| Globale `CLAUDE.md` (`/root/CLAUDE.md`) | JEDE Session | **Kein Wort über Brix!** |

Die globale CLAUDE.md ist die einzige Datei die Claude in JEDER Session liest — egal welches Repo. Ohne Brix-Abschnitt dort weiß Claude nichts.

**Fix:** Abschnitt in `/root/CLAUDE.md` ergänzt:
- `brix` CLI ist verfügbar
- Wann Brix nutzen (>3 Tool-Calls)
- Verfügbare Pipelines und Commands
- Pfad-Konvention `/host/root/`
- Strategie-Wahl (targeted vs broad)
- Skills und Repo-Pfade

**For users:** Brix muss an DREI Stellen registriert werden:
1. **Globale CLAUDE.md** (`~/CLAUDE.md` oder `/root/CLAUDE.md`) — damit Claude es in jeder Session kennt
2. **Skills global** (`~/.claude/commands/`) — damit User `/download-attachments` tippen können
3. **Wrapper-Script** (`/usr/local/bin/brix`) — damit der CLI-Aufruf funktioniert

Nur Skills allein reichen nicht — Claude nutzt Skills nur wenn der User explizit `/` tippt. Die globale CLAUDE.md ist der einzige Weg Claude proaktiv auf Brix aufmerksam zu machen.

---

## 10. Claude braucht Konventions-Dokumentation, nicht nur Tool-Dokumentation

**Problem:** Eine andere Claude-Session versuchte eine neue Brix-Pipeline zu erstellen. Drei Fehler:
1. Helper-Script nutzte nur `sys.stdin.read()` statt das argv+stdin Pattern → Crash
2. Pipeline setzte `concurrency: "{{ input.x }}"` (Jinja2-Template) → Pydantic-Validierungsfehler (muss int sein)
3. Claude baute den Container neu nach Hinzufügen von Helpers → unnötig, Volumes sind gemountet

**Intent:** Claude muss nicht nur wissen WAS Brix kann, sondern WIE man es richtig nutzt. Konventionen, Patterns, Pitfalls — das gehört in die CLAUDE.md.

**Root cause:** Die globale CLAUDE.md dokumentierte nur Commands und Pipelines, nicht die Konventionen für Helper-Scripts, Pipeline-YAML-Regeln und den Workflow. Claude musste durch Trial-and-Error lernen was die existierenden Helpers schon wussten.

**Fix:** Globale CLAUDE.md erweitert um:
- Helper-Boilerplate (argv+stdin Pattern, komplett copy-paste-fähig)
- Pipeline-YAML-Regeln (concurrency=int, Host-Pfade, default-Filter)
- Expliziter Hinweis: "KEIN Container-Rebuild nötig für pipelines/ und helpers/"
- Workflow: validate → dry-run → run

**For users:** Dokumentiere nicht nur was dein Tool kann, sondern wie man damit arbeitet. Claude ist ein guter Entwickler, aber er braucht die Konventionen explizit — er kann sie nicht aus dem Code ableiten wenn er den Code nicht liest. Die CLAUDE.md ist die "Onboarding-Doku für jeden neuen Claude-Entwickler".

---

## 11. Base64 in foreach = Payload-Explosion

**Problem:** Pipeline liest 80 PDFs, kodiert sie als base64, schickt sie per foreach HTTP-Step an MarkItDown. 80 × 200KB base64 = 16MB JSON durch die Pipeline. Crash.

**Intent:** Große Binärdaten dürfen nie durch die JSON-Datenfluss-Kette. Dateien per Pfad referenzieren, nicht inline.

**Fix:** Helper-Script liest Dateien direkt (Pfade statt base64), macht HTTP-Calls intern mit httpx. foreach über Pfad-Referenzen (`{"path": "/host/root/file.pdf", "size": 12345}`), nicht über Dateiinhalte.

**For users:** Wenn deine Pipeline große Dateien verarbeitet: Pfade übergeben, nicht Inhalte. Python-Helper mit httpx.AsyncClient für Batch-HTTP ist besser als foreach-HTTP mit base64-Payloads.

---

## 12. Container-Netzwerk: Services per Name erreichbar

**Problem:** Claude wusste nicht dass MarkItDown (`markitdown-mcp:8081`) aus dem Brix-Container erreichbar ist. Versuchte curl (nicht installiert). Unnötiger Debug-Aufwand.

**Intent:** Brix ist im shared-network — alle anderen Container sind per Name erreichbar. Das muss dokumentiert sein.

**For users:** Brix kann Services im selben Docker-Netzwerk per Name ansprechen. Zum Debuggen: `docker exec brix python3 -c "import httpx; print(httpx.get('http://service:port/health').text)"`

---

## 13. MCP Server Registration: claude mcp add

**Problem:** Brix v2 MCP Server läuft (Port 8091), aber Claude Code weiß nichts davon. MCP-Tools sind unsichtbar. Kein Hinweis in der Dokumentation wie man den Server registriert.

**Intent:** Brix v2 MCP Server muss in Claude Code registriert werden damit die Tools verfügbar sind. Das ist der wichtigste Setup-Schritt — ohne ihn ist v2 nutzlos.

**Lösung:**
```bash
claude mcp add brix -- docker exec -i brix-mcp brix mcp
```

Danach neue Session starten. Claude sieht automatisch alle `mcp__brix__*` Tools.

**Voraussetzung:** brix-mcp Container muss laufen (`docker compose up -d`).

**For users:** Das ist EIN Befehl. Danach hat Claude 14+ MCP-Tools für Pipeline-Management. Aber: Pipelines müssen in `~/.brix/pipelines/` liegen (kopieren von `/app/pipelines/` oder Symlink).

---

## 14. v2 MCP Performance = v1 CLI Performance

**Problem:** Befürchtung dass MCP-Transport langsamer ist als direkte CLI-Ausführung.

**Messung:** Gleicher Use Case (50 Rechnungs-PDFs, broad strategy, top=400):
- v1 CLI (brix run via Bash): ~35s
- v2 MCP (mcp__brix__run_pipeline): 35.4s

**Ergebnis:** Kein messbarer Overhead. MCP-Transport (stdio JSON-RPC) ist vernachlässigbar gegenüber Pipeline-Laufzeit.

**Zusätzlicher Vorteil MCP:** Token-Einsparung ~3.000 statt ~5.000 (CLI) weil MCP-Response strukturiert ist und kein JSON-Parsing aus Bash-stdout nötig.

---

## 15. Pipeline-Pfade: Container-Volume vs. User-Pipelines

**Problem:** MCP Server sucht Pipelines in `~/.brix/pipelines/`. Pipeline-Dateien liegen aber in `/app/pipelines/` (Docker Volume-Mount aus dem Repo).

**Workaround:** Pipelines manuell kopieren:
```bash
cp /root/docker/brix/pipelines/*.yaml ~/.brix/pipelines/
```

**Eigentlicher Fix:** PipelineStore soll beide Pfade durchsuchen (Task T-BRIX-V2-19).

**For users:** Bis zum Fix: Pipelines müssen in `~/.brix/pipelines/` liegen damit MCP-Tools und REST API sie finden.

---

## General Principle

Brix integrates with the existing system rather than creating its own isolated environment. This means:
- **Use host binaries** instead of installing copies
- **Use existing MCP server auth** instead of duplicating credentials
- **Use Docker networking** (exec, socket) instead of HTTP bridges
- **Stay transparent** — the wrapper script, the stdio bridge, the volume mounts all make Brix invisible to Claude
