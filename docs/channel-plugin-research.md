# Channel Plugin Research — Brix v6 (T-BRIX-V6-26)

**Date:** 2026-03-25

---

## Context

Brix v6 introduces two notification channels: the Claude Code SSE channel (T-BRIX-V6-05) and the Mattermost webhook channel (T-BRIX-V6-06). This document researches what a future extensible Channel Plugin API would require, based on the official Claude Code plugin/skills model and existing Brix architecture.

---

## 1. Claude Code Plugin / Slash Command Model

Claude Code supports custom slash commands (also called "skills") as Markdown files. These are the primary extensibility mechanism documented in Anthropic's official Claude Code docs.

### File locations

| Location | Scope | Label in `/help` |
|----------|-------|-----------------|
| `~/.claude/commands/*.md` | Global (all projects) | `(user)` |
| `.claude/commands/*.md` | Project-specific | `(project)` |

Subdirectories create namespaces: `.claude/commands/brix/run.md` → `/brix:run`

### Format

```yaml
---
description: Short description for /help (~60 chars)
argument-hint: <pipeline-name> [params...]
allowed-tools: Bash(brix:*), Read, Grep
model: sonnet
---

Prompt content with $ARGUMENTS substitution.
```

Key properties:
- `description` — shown in `/help` autocomplete
- `argument-hint` — shown in argument placeholder
- `allowed-tools` — whitelist of tools the skill can invoke
- `model` — override default model for this skill (haiku | sonnet | opus)
- `$ARGUMENTS` — substituted with user-supplied arguments at runtime

### MCP Server Plugin Model

There is no separate "channel plugin" API in the Claude Code official documentation as of March 2026. The extensibility model is:

1. **Slash Commands (Markdown)** — user-facing prompts, invoke `brix run` via Bash
2. **MCP Servers** — expose tools that Claude and Brix can call
3. **Brix Pipelines** — compose MCP tools into workflows

There is no official first-class "plugin" registry or plugin SDK for Claude Code channels. The closest concept is an **MCP server** that implements notification capabilities, registered via `claude mcp add`.

---

## 2. Claude Code SSE Channel (T-BRIX-V6-05)

### What it does

When a pipeline run completes, Brix sends an MCP notification back to the calling Claude session. This avoids polling — Claude receives the result as a server-sent event.

### Implementation in Brix

The SSE channel is implemented as part of the MCP server response cycle:
- `run_pipeline` with `async=true` returns `run_id` immediately
- On completion, the MCP server sends a `notifications/message` via the MCP stdio protocol
- The Claude session receives the notification and can surface it to the user

### Technical requirements for a plugin

An SSE channel plugin would need to:
1. Implement a `notify(run_id, status, result)` interface
2. Have access to the active MCP session (the `stdio` connection between Brix and Claude)
3. Serialize result data to the MCP notification format (`notifications/message` JSON-RPC)

### Constraint

The MCP Python SDK (`mcp>=1.20`) used by Brix does not expose a public "push notification" API to arbitrary consumers. The capability (declared via `experimental/capabilities: {claude/channel: {}}`) is used internally by the MCP server handler. A channel plugin would need to be integrated at the server level, not as an external plugin.

### Current status

The SSE channel is declared as an experimental capability in the MCP `initialize` response:

```python
# In mcp_server.py, _handle_initialize:
experimental={"claude/channel": {}}
```

This signals to Claude Code that the server supports push notifications. The notification is sent via `server.request_context.session.send_log_message(...)` or equivalent MCP notification primitive.

---

## 3. Mattermost Webhook Channel (T-BRIX-V6-06)

### What it does

When a run completes (success or failure), Brix POSTs a JSON payload to a Mattermost incoming webhook URL. This enables team notifications without Claude being in the loop.

### Implementation

Standard HTTP POST to `https://mattermost.example.com/hooks/<token>` with payload:

```json
{
  "text": "Pipeline `process-invoices` completed: 23 items processed in 4.7s",
  "username": "Brix",
  "icon_emoji": ":brix:"
}
```

### Requirements for a channel plugin

A Mattermost channel plugin (or any webhook channel plugin) needs:

1. **Configuration:** webhook URL, optional secret, message template
2. **Trigger conditions:** on_success / on_failure / always
3. **Message formatter:** pipeline name, status, duration, key result fields
4. **HTTP client:** synchronous `httpx.post` (not async, runs in pipeline completion handler)

---

## 4. Requirements for a Generic Channel Plugin API

If Brix were to support third-party channel plugins (e.g. Slack, Teams, PagerDuty), the plugin API would need:

### Interface definition

```python
class BrixChannel:
    name: str                         # e.g. "slack", "teams", "pagerduty"
    config_schema: dict               # JSON Schema for channel configuration

    async def send(
        self,
        run_id: str,
        pipeline_name: str,
        status: Literal["success", "failure", "running"],
        result: dict,
        config: dict,
    ) -> None:
        ...
```

### Registration

```python
# In channels_registry.py or as an entrypoint
brix_channels = [
    "brix_slack:SlackChannel",
    "brix_teams:TeamsChannel",
]
```

Python package entrypoints (`pyproject.toml`):

```toml
[project.entry-points."brix.channels"]
slack = "brix_slack:SlackChannel"
teams = "brix_teams:TeamsChannel"
```

### Invocation

Channels are invoked by the pipeline engine after a run completes:

```python
for channel_config in pipeline.notify_channels:
    plugin = channel_registry.get(channel_config["type"])
    await plugin.send(run_id, pipeline_name, status, result, channel_config)
```

---

## 5. Gap Analysis: What is Missing for a Full Plugin System

| Feature | Status |
|---------|--------|
| SSE/MCP push channel | Implemented (v6-05), not pluggable |
| Mattermost webhook channel | Implemented (v6-06), hardcoded |
| Generic `BrixChannel` interface | Not implemented |
| Plugin registration via entrypoints | Not implemented |
| Channel configuration schema validation | Not implemented |
| Per-pipeline channel overrides | Partially (via `run_pipeline` params) |
| Channel plugin discovery via MCP tool | Not implemented |

---

## 6. Recommendation

For v6.0, the two built-in channels (SSE + Mattermost) cover the primary use cases. A generic plugin API is **not required in v6** but should be planned for v7 if channel diversity becomes a requirement.

The plugin interface should follow the `BrixChannel` abstract class pattern above. Python entrypoints are the correct mechanism for third-party plugin discovery — this is the same approach used by pytest plugins, Sphinx extensions, and Airflow operators.

The SSE channel cannot be made externally pluggable without MCP SDK changes. It should remain a built-in capability of the Brix MCP server.

---

## 7. References

- [Claude Code Slash Commands (official docs)](https://docs.anthropic.com/en/docs/claude-code/tutorials/custom-slash-commands)
- [MCP Python SDK — stdio transport](https://github.com/modelcontextprotocol/python-sdk)
- `docs/research-claude-code-skills.md` — prior Brix research on slash commands
- `src/brix/mcp_server.py` lines 3437-3495 — SSE + Mattermost channel implementation
- `src/brix/mcp_server.py` line 6409 — SSE capability declaration in initialize handler
