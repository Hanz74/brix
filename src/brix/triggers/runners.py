"""Trigger runners — poll sources and fire pipelines."""
import asyncio
import hashlib
import imaplib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from brix.triggers.models import TriggerConfig
from brix.triggers.state import TriggerState
from brix.loader import PipelineLoader
from brix.engine import PipelineEngine
from brix.pipeline_store import PipelineStore
from brix.runners.mcp import McpRunner
from brix.credential_store import CredentialStore, CredentialNotFoundError
from brix.config import config


class BaseTriggerRunner:
    """Base class for all trigger runners."""

    def __init__(self, trigger: TriggerConfig, state: TriggerState):
        self.trigger = trigger
        self.state = state
        self.loader = PipelineLoader()

    async def poll(self) -> list[dict]:
        """Poll source, return list of trigger events."""
        raise NotImplementedError

    def dedupe(self, events: list[dict]) -> list[dict]:
        """Filter out already-processed events."""
        if not self.trigger.dedupe_key:
            return events
        new_events = []
        for event in events:
            key = self.loader.render_template(self.trigger.dedupe_key, {"trigger": event})
            if not self.state.is_deduped(self.trigger.id, key):
                new_events.append(event)
        return new_events

    async def fire(self, event: dict):
        """Run the target pipeline with trigger event as input."""
        # Render params with trigger context
        rendered_params = {}
        for key, template in self.trigger.params.items():
            if isinstance(template, str) and "{{" in template:
                rendered_params[key] = self.loader.render_template(template, {"trigger": event})
            else:
                rendered_params[key] = template

        # Run pipeline
        store = PipelineStore()
        try:
            pipeline = store.load(self.trigger.pipeline)
        except FileNotFoundError:
            print(f"[trigger:{self.trigger.id}] Pipeline '{self.trigger.pipeline}' not found")
            return None

        engine = PipelineEngine()
        result = await engine.run(pipeline, rendered_params)

        # Record dedup
        if self.trigger.dedupe_key:
            key = self.loader.render_template(self.trigger.dedupe_key, {"trigger": event})
            self.state.record_fired(self.trigger.id, key, result.run_id)

        return result


class MailTriggerRunner(BaseTriggerRunner):
    """Polls for new mails — supports M365 (MCP) and IMAP providers."""

    async def poll(self) -> list[dict]:
        provider = self.trigger.provider
        if provider == "imap":
            return await self._poll_imap()
        # Default: m365
        return await self._poll_m365()

    async def _poll_m365(self) -> list[dict]:
        """Poll M365 for new mails via MCP list-mail-messages."""
        # Build MCP call params from trigger filter
        params = dict(self.trigger.filter)
        if self.trigger.filter.get("unread"):
            params.setdefault("filter", "isRead eq false")
            del params["unread"]

        # Call M365 MCP server
        runner = McpRunner()

        class FakeStep:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)

        step = FakeStep(server="m365", tool="list-mail-messages", params=params, timeout="30s")
        try:
            result = await runner.execute(step, context=None)
        except Exception as e:
            print(f"[trigger:{self.trigger.id}] Mail poll error: {e}")
            return []

        if not result.get("success"):
            return []

        data = result.get("data", {})
        mails = data.get("value", []) if isinstance(data, dict) else []

        # Convert to trigger events
        events = []
        for mail in mails:
            events.append({
                "message_id": mail.get("id", ""),
                "subject": mail.get("subject", ""),
                "from": mail.get("from", {}).get("emailAddress", {}).get("address", ""),
                "received": mail.get("receivedDateTime", ""),
                "has_attachments": mail.get("hasAttachments", False),
            })

        return events

    async def _poll_imap(self) -> list[dict]:
        """Poll an IMAP server for unseen messages since last_check.

        Uses imaplib (stdlib). Credentials are resolved from CredentialStore
        via config.app_password_credential (UUID).
        """
        email_addr = self.trigger.email
        cred_ref = self.trigger.app_password_credential
        folder = self.trigger.folder or "INBOX"
        server = self.trigger.server or "imap.gmail.com"

        if not email_addr:
            print(f"[trigger:{self.trigger.id}] IMAP: 'email' is required for provider=imap")
            return []
        if not cred_ref:
            print(f"[trigger:{self.trigger.id}] IMAP: 'app_password_credential' is required for provider=imap")
            return []

        # Resolve app password from CredentialStore
        try:
            cred_store = CredentialStore()
            app_password = cred_store.resolve(cred_ref)
        except CredentialNotFoundError as e:
            print(f"[trigger:{self.trigger.id}] IMAP credential error: {e}")
            return []
        except Exception as e:
            print(f"[trigger:{self.trigger.id}] IMAP credential resolution failed: {e}")
            return []

        # Build SINCE date from last_check state (or use today as fallback)
        last_check = self.state.get_last_check(self.trigger.id)
        if last_check:
            # imaplib SEARCH SINCE expects DD-Mon-YYYY format
            since_dt = datetime.fromtimestamp(last_check, tz=timezone.utc)
        else:
            since_dt = datetime.now(timezone.utc)
        since_str = since_dt.strftime("%d-%b-%Y")

        try:
            loop = asyncio.get_event_loop()
            events = await loop.run_in_executor(
                None,
                lambda: self._imap_search(server, email_addr, app_password, folder, since_str),
            )
        except Exception as e:
            print(f"[trigger:{self.trigger.id}] IMAP poll error: {e}")
            return []

        # Update last_check timestamp
        self.state.set_last_check(self.trigger.id, time.time())

        return events

    def _imap_search(
        self,
        server: str,
        email_addr: str,
        password: str,
        folder: str,
        since_str: str,
    ) -> list[dict]:
        """Synchronous IMAP search — run in executor.

        Logs in, selects folder, searches for UNSEEN messages SINCE since_str,
        and returns a list of trigger event dicts (one per unseen message).
        """
        mail = imaplib.IMAP4_SSL(server)
        try:
            mail.login(email_addr, password)
            status, _ = mail.select(folder, readonly=True)
            if status != "OK":
                print(f"[trigger] IMAP: could not SELECT folder '{folder}' (status={status})")
                return []

            search_criterion = f"UNSEEN SINCE {since_str}"
            status, data = mail.search(None, search_criterion)
            if status != "OK":
                return []

            # data[0] is a space-separated list of message UIDs
            message_ids = data[0].split() if data and data[0] else []
            count = len(message_ids)

            if count == 0:
                return []

            # Return one event per unseen message with UID; fetch basic headers
            events = []
            for uid in message_ids:
                uid_str = uid.decode() if isinstance(uid, bytes) else str(uid)
                events.append({
                    "message_id": uid_str,
                    "uid": uid_str,
                    "folder": folder,
                    "server": server,
                    "email": email_addr,
                    "unseen_count": count,
                })
            return events
        finally:
            try:
                mail.logout()
            except Exception:
                pass


class FileTriggerRunner(BaseTriggerRunner):
    """Polls a directory for new/changed files."""

    async def poll(self) -> list[dict]:
        path = Path(self.trigger.path) if self.trigger.path else None
        if not path or not path.exists():
            return []

        pattern = self.trigger.pattern or "*"
        events = []

        for f in path.glob(pattern):
            if f.is_file():
                stat = f.stat()
                events.append({
                    "path": str(f),
                    "filename": f.name,
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                })

        return events


class HttpPollTriggerRunner(BaseTriggerRunner):
    """Polls an HTTP endpoint for changes."""

    async def poll(self) -> list[dict]:
        import httpx

        url = self.trigger.url
        if not url:
            return []

        headers = dict(self.trigger.headers)
        # Render env vars in headers
        for k, v in headers.items():
            if "{{" in v:
                headers[k] = self.loader.render_template(v, {"env": dict(os.environ)})

        try:
            async with httpx.AsyncClient(timeout=config.HTTP_POLL_TIMEOUT) as client:
                response = await client.get(url, headers=headers)

            if response.status_code >= 400:
                return []

            try:
                data = response.json()
            except Exception:
                data = response.text

            # Compute hash
            if self.trigger.hash_field and isinstance(data, dict):
                # Simple dot-notation field access
                hash_value = data
                for part in self.trigger.hash_field.lstrip("$.").split("."):
                    hash_value = hash_value.get(part, "") if isinstance(hash_value, dict) else ""
                content_hash = hashlib.sha256(str(hash_value).encode()).hexdigest()[:16]
            else:
                content_hash = hashlib.sha256(
                    json.dumps(data, sort_keys=True, default=str).encode()
                ).hexdigest()[:16]

            return [{
                "payload": data,
                "hash": content_hash,
                "url": url,
                "status_code": response.status_code,
            }]

        except Exception as e:
            print(f"[trigger:{self.trigger.id}] HTTP error: {e}")
            return []


class PipelineDoneTriggerRunner(BaseTriggerRunner):
    """Fires when a referenced pipeline completes."""

    async def poll(self) -> list[dict]:
        pipeline_name = self.trigger.filter.get("pipeline") or self.trigger.pipeline_target or ""
        status_filter = self.trigger.status or "any"

        events = self.state.get_unprocessed_events(pipeline_name, status_filter)

        trigger_events = []
        for event in events:
            # Parse result and input from JSON
            import json as _json
            result = {}
            if event.get("result_json"):
                try:
                    result = _json.loads(event["result_json"])
                except Exception:
                    result = {}
            source_input = {}
            if event.get("input_json"):
                try:
                    source_input = _json.loads(event["input_json"])
                except Exception:
                    source_input = {}

            # Build source_run context (Sub-Feature 4: Trigger Context)
            source_run = {
                "run_id": event["run_id"],
                "pipeline_name": event["pipeline_name"],
                "status": event["status"],
                "result": result,
                "input": source_input,
            }

            # Sub-Feature 1: Input-Filter — skip if source run didn't have these input params
            if self.trigger.input_filter:
                match = all(
                    source_input.get(k) == v
                    for k, v in self.trigger.input_filter.items()
                )
                if not match:
                    self.state.mark_event_processed(event["id"])
                    continue

            trigger_events.append({
                "run_id": event["run_id"],
                "pipeline_name": event["pipeline_name"],
                "status": event["status"],
                "event_id": event["id"],
                "source_run": source_run,
            })
            self.state.mark_event_processed(event["id"])

        return trigger_events

    def _passes_when(self, event: dict) -> bool:
        """Sub-Feature 2: evaluate `when` expression against trigger context."""
        if not self.trigger.when:
            return True
        context = {"trigger": {"source_run": event.get("source_run", {})}}
        return self.loader.evaluate_condition(self.trigger.when, context)

    def _build_forward_input(self, event: dict) -> dict:
        """Sub-Feature 3: build forwarded input params from source_run context."""
        if not self.trigger.forward_input:
            return {}
        context = {"trigger": {"source_run": event.get("source_run", {})}}
        result = {}
        for key, template in self.trigger.forward_input.items():
            if isinstance(template, str) and "{{" in template:
                result[key] = self.loader.render_template(template, context)
            else:
                result[key] = template
        return result

    async def fire(self, event: dict):
        """Extended fire: apply when-guard, forward_input, and multi-pipeline."""
        # Sub-Feature 2: when guard
        if not self._passes_when(event):
            return None

        # Sub-Feature 3: forward_input merged into base params
        forwarded = self._build_forward_input(event)

        # Sub-Feature 5: Multi-Pipeline Trigger
        if self.trigger.pipelines:
            results = []
            for pipe_spec in self.trigger.pipelines:
                pipe_name = pipe_spec.get("pipeline") or pipe_spec.get("name", "")
                extra_params = pipe_spec.get("params", {})
                if not pipe_name:
                    continue
                # Render params with trigger context
                rendered = {}
                ctx = {"trigger": event}
                for k, tmpl in extra_params.items():
                    if isinstance(tmpl, str) and "{{" in tmpl:
                        rendered[k] = self.loader.render_template(tmpl, ctx)
                    else:
                        rendered[k] = tmpl
                rendered.update(forwarded)

                store = PipelineStore()
                try:
                    pipeline = store.load(pipe_name)
                except FileNotFoundError:
                    print(f"[trigger:{self.trigger.id}] Pipeline '{pipe_name}' not found")
                    continue
                engine = PipelineEngine()
                result = await engine.run(pipeline, rendered)
                results.append(result)
                if self.trigger.dedupe_key:
                    key = self.loader.render_template(self.trigger.dedupe_key, {"trigger": event})
                    self.state.record_fired(self.trigger.id, key, result.run_id)
            return results[-1] if results else None

        # Single-pipeline fire — merge forwarded into base params
        base_params = {}
        ctx = {"trigger": event}
        for key, template in self.trigger.params.items():
            if isinstance(template, str) and "{{" in template:
                base_params[key] = self.loader.render_template(template, ctx)
            else:
                base_params[key] = template
        base_params.update(forwarded)

        store = PipelineStore()
        try:
            pipeline = store.load(self.trigger.pipeline)
        except FileNotFoundError:
            print(f"[trigger:{self.trigger.id}] Pipeline '{self.trigger.pipeline}' not found")
            return None

        engine = PipelineEngine()
        result = await engine.run(pipeline, base_params)

        if self.trigger.dedupe_key:
            key = self.loader.render_template(self.trigger.dedupe_key, {"trigger": event})
            self.state.record_fired(self.trigger.id, key, result.run_id)

        return result


# Registry
TRIGGER_RUNNERS = {
    "mail": MailTriggerRunner,
    "file": FileTriggerRunner,
    "http_poll": HttpPollTriggerRunner,
    "pipeline_done": PipelineDoneTriggerRunner,
}
