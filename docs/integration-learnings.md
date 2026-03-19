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

## General Principle

Brix integrates with the existing system rather than creating its own isolated environment. This means:
- **Use host binaries** instead of installing copies
- **Use existing MCP server auth** instead of duplicating credentials
- **Use Docker networking** (exec, socket) instead of HTTP bridges
- **Stay transparent** — the wrapper script, the stdio bridge, the volume mounts all make Brix invisible to Claude
