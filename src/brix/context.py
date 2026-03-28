"""Pipeline execution context — holds state, outputs, credentials."""
import hashlib
import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

from brix.models import Pipeline
from brix.credential_store import CredentialStore, is_credential_uuid, CredentialNotFoundError
from brix.config import config

WORKDIR_BASE = Path.home() / ".brix" / "runs"
CACHE_BASE = Path.home() / ".brix" / "cache" / "steps"

# Module-level reference to the active McpConnectionPool, set by the engine
# when a pipeline run starts and cleared when it finishes.  Used by
# brix__server_health and the mcp_server_down alert condition to access
# health data without coupling those modules directly to the engine.
_active_pool: "Any | None" = None


class CacheManager:
    """Content-addressed cache for step outputs (T-BRIX-V6-24).

    Cache key: SHA256 of ``step_id + JSON(resolved_params)``.
    Each entry is a JSON file stored under ``~/.brix/cache/steps/<hash>.json``.
    """

    def __init__(self, cache_dir: Path = None):
        self._cache_dir = cache_dir or CACHE_BASE
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def compute_key(self, step_id: str, params: Any) -> str:
        """Return the hex SHA256 hash for (step_id, params)."""
        payload = json.dumps({"step_id": step_id, "params": params}, sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()

    def _entry_path(self, key: str) -> Path:
        return self._cache_dir / f"{key}.json"

    def get(self, step_id: str, params: Any) -> "Any | None":
        """Return cached output for (step_id, params), or ``None`` on miss."""
        key = self.compute_key(step_id, params)
        path = self._entry_path(key)
        if not path.exists():
            return None
        try:
            entry = json.loads(path.read_text())
            return entry.get("output")
        except (json.JSONDecodeError, OSError):
            return None

    def set(self, step_id: str, params: Any, output: Any) -> None:
        """Store *output* for (step_id, params) in the cache."""
        key = self.compute_key(step_id, params)
        path = self._entry_path(key)
        entry = {
            "step_id": step_id,
            "key": key,
            "output": output,
        }
        try:
            path.write_text(json.dumps(entry, default=str))
        except (OSError, TypeError, ValueError):
            pass  # Non-fatal: step will run normally on next invocation

    def invalidate(self, step_id: str, params: Any) -> None:
        """Remove a specific cache entry."""
        key = self.compute_key(step_id, params)
        path = self._entry_path(key)
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass

# Outputs with more items than this threshold are stored as JSONL files on disk
# instead of keeping the full list in RAM and as a JSON blob.
# Lowered from 1000 → 100 to prevent OOM on large attachment payloads (INBOX-347).
LARGE_OUTPUT_THRESHOLD = config.LARGE_OUTPUT_THRESHOLD

# If a serialized output exceeds this byte size it is also spilled to JSONL,
# regardless of item count.  Prevents OOM when items are individually large
# (e.g. mails with base64-encoded attachments).
LARGE_OUTPUT_SIZE_BYTES = config.large_output_size_bytes


class PipelineContext:
    """Holds pipeline execution state."""

    def __init__(
        self,
        pipeline_input: dict = None,
        credentials: dict = None,
        workdir: Path = None,
        resume_from: str = None,
        run_id: str = None,
    ):
        self.run_id = run_id or f"run-{uuid.uuid4().hex[:12]}"
        self.input = pipeline_input or {}
        self.credentials = credentials or {}
        self.step_outputs: dict[str, Any] = {}  # step_id → output
        self.step_progress: dict[str, dict] = {}  # step_id → last BRIX_PROGRESS payload
        self.workdir = workdir or (WORKDIR_BASE / self.run_id)
        self.workdir.mkdir(parents=True, exist_ok=True)
        (self.workdir / "step_outputs").mkdir(exist_ok=True)
        (self.workdir / "files").mkdir(exist_ok=True)
        self._resume_from = resume_from
        self._jinja_cache: dict | None = None
        self._secret_values: set[str] = set()  # plaintext secret variable values (T-BRIX-DB-26)

    @classmethod
    def from_pipeline(
        cls,
        pipeline: Pipeline,
        user_input: dict = None,
        run_id: str = None,
        profile: str = None,
    ) -> "PipelineContext":
        """Create context from a Pipeline model.

        Resolves credentials from environment variables.
        Merges user_input with pipeline defaults.

        If *profile* is provided (or a profile is active via the ``BRIX_PROFILE``
        env var or ``default_profile`` in ``~/.brix/profiles.yaml``), the
        profile's env vars are injected into ``os.environ`` before credential
        resolution, and its ``input_defaults`` fill gaps not covered by either
        user_input or pipeline defaults.
        """
        # Apply active profile: inject env vars + collect input_defaults
        from brix.profiles import ProfileManager

        mgr = ProfileManager()
        active = mgr.active_profile_name(override=profile)
        profile_config = mgr.apply_profile(active)
        profile_input_defaults: dict[str, Any] = profile_config.get("input_defaults", {})

        # Merge input (priority: user_input > pipeline defaults > profile defaults)
        resolved_input: dict[str, Any] = {}
        for key, param in pipeline.input.items():
            if user_input and key in user_input:
                resolved_input[key] = user_input[key]
            elif param.default is not None:
                resolved_input[key] = param.default
            elif key in profile_input_defaults:
                resolved_input[key] = profile_input_defaults[key]

        # Resolve credentials from ENV or CredentialStore (UUID refs), with optional OAuth2 refresh
        resolved_credentials: dict[str, Any] = {}
        for key, cred in pipeline.credentials.items():
            if is_credential_uuid(cred.env):
                # UUID-based credential — resolve from CredentialStore
                try:
                    store = CredentialStore()
                    value = store.resolve(cred.env)
                except CredentialNotFoundError:
                    import warnings
                    warnings.warn(
                        f"Credential UUID '{cred.env}' not found in store for key '{key}'. "
                        "Using empty string.",
                        UserWarning,
                        stacklevel=2,
                    )
                    value = ""
            else:
                value = os.environ.get(cred.env, "")
            if getattr(cred, "refresh", None):
                value = cls._refresh_credential(cred, value)
            resolved_credentials[key] = value

        ctx = cls(pipeline_input=resolved_input, credentials=resolved_credentials, run_id=run_id)
        ctx._active_profile = active  # Store for introspection
        return ctx

    @staticmethod
    def _refresh_credential(cred: Any, current_value: str) -> str:
        """Check OAuth2 token expiry and refresh if needed (T-BRIX-V4-13).

        Returns the (possibly refreshed) token.  All errors are silently swallowed
        so that a misconfigured refresh block never breaks a pipeline run.
        """
        # Try to decode JWT to check expiry — PyJWT may not be installed
        try:
            import jwt  # PyJWT
            import time as _time

            decoded = jwt.decode(current_value, options={"verify_signature": False})
            exp = decoded.get("exp", 0)
            if exp - _time.time() > 300:  # More than 5 minutes left
                return current_value  # Still valid — no refresh needed
        except ImportError:
            pass  # PyJWT not available — attempt refresh anyway
        except Exception:
            pass  # Not a JWT or decode error — attempt refresh anyway

        # Only oauth2_client_credentials supported for now
        refresh = cred.refresh
        if not isinstance(refresh, dict) or refresh.get("type") != "oauth2_client_credentials":
            return current_value

        token_url = refresh.get("token_url", "")
        client_id = os.environ.get(refresh.get("client_id_env", ""), "")
        client_secret = os.environ.get(refresh.get("client_secret_env", ""), "")
        scope = refresh.get("scope", "")

        if not all([token_url, client_id, client_secret]):
            return current_value

        try:
            import httpx

            resp = httpx.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": scope,
                },
                timeout=15,
            )
            if resp.status_code == 200:
                new_token = resp.json().get("access_token", "")
                if new_token:
                    os.environ[cred.env] = new_token
                    return new_token
        except Exception:
            pass  # Network error, httpx not available, etc.

        return current_value

    def update_step_progress(self, step_id: str, progress: dict) -> None:
        """Store the latest BRIX_PROGRESS payload for a step (T-BRIX-V4-BUG-05).

        Persists to step_progress.json in the workdir so get_run_status can
        read it for async / background runs.
        """
        import time as _time
        entry = dict(progress)
        # Compute derived fields
        processed = entry.get("processed", 0)
        total = entry.get("total", 0)
        if total > 0:
            entry["percent"] = round(processed / total * 100, 1)
        else:
            entry["percent"] = 0.0
        entry["_updated_at"] = _time.time()
        self.step_progress[step_id] = entry
        # Persist to disk for external polling
        sp_path = self.workdir / "step_progress.json"
        try:
            sp_path.write_text(json.dumps(self.step_progress, default=str))
        except (OSError, TypeError, ValueError):
            pass  # Non-fatal: progress won't be visible via polling but run continues

    def validate_output_schema(self, step_id: str, output: Any, output_schema: dict) -> None:
        """Warn (non-blocking) when output is missing fields declared in output_schema.

        Performs simple field-existence check against the dict output.
        Only checks top-level keys listed in output_schema.  If output is not a
        dict or output_schema is empty, this is a no-op.
        """
        if not output_schema:
            return
        if not isinstance(output, dict):
            import warnings
            warnings.warn(
                f"Step '{step_id}': output_schema defined but output is not a dict "
                f"(got {type(output).__name__}) — schema check skipped",
                UserWarning,
                stacklevel=2,
            )
            return
        missing = [k for k in output_schema if k not in output]
        if missing:
            import warnings
            warnings.warn(
                f"Step '{step_id}': output is missing schema fields: {missing}",
                UserWarning,
                stacklevel=2,
            )

    def set_output(self, step_id: str, output: Any, output_schema: dict = None) -> None:
        """Store step output. Large lists go to JSONL file, small values stay in RAM.

        Spills to JSONL when either:
        - The items list has more than LARGE_OUTPUT_THRESHOLD entries, OR
        - The serialized JSON representation exceeds LARGE_OUTPUT_SIZE_BYTES (10 MB),
          regardless of item count — prevents OOM on large per-item payloads such as
          emails with base64-encoded attachments (INBOX-347).

        If *output_schema* is provided, the output is validated against it
        (field-existence check, warning only — T-BRIX-V6-13).
        """
        if output_schema:
            self.validate_output_schema(step_id, output, output_schema)
        self._jinja_cache = None  # Invalidate cache on any output change
        if isinstance(output, dict) and "items" in output:
            items = output.get("items", [])
            if isinstance(items, list) and len(items) > 0:
                # Check item-count threshold first (cheap).
                over_count = len(items) > LARGE_OUTPUT_THRESHOLD
                # Check size threshold only when item count is within limit (avoids
                # double-serialisation for the common large-count case).
                over_size = False
                if not over_count:
                    try:
                        serialized = json.dumps(items, default=str)
                        over_size = len(serialized.encode()) > LARGE_OUTPUT_SIZE_BYTES
                    except (TypeError, ValueError):
                        pass

                if over_count or over_size:
                    # Write items to JSONL, keep only a lightweight reference in RAM
                    jsonl_path = self.workdir / "step_outputs" / f"{step_id}.jsonl"
                    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(jsonl_path, "w") as f:
                        for item in items:
                            f.write(json.dumps(item, default=str) + "\n")

                    # RAM entry: reference + summary only (no items array)
                    self.step_outputs[step_id] = {
                        "_large_output": True,
                        "_jsonl_path": str(jsonl_path),
                        "_count": len(items),
                        "summary": output.get("summary", {}),
                    }
                    return

        # Small outputs: normal behaviour — keep in RAM and persist as JSON
        self.step_outputs[step_id] = output
        output_file = self.workdir / "step_outputs" / f"{step_id}.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            output_file.write_text(json.dumps(output, default=str))
        except (TypeError, ValueError):
            pass  # Non-serializable output — skip persistence

    def get_output(self, step_id: str) -> Any:
        """Get a step's output. Transparently loads from JSONL when the output is large."""
        output = self.step_outputs.get(step_id)
        if isinstance(output, dict) and output.get("_large_output"):
            jsonl_path = output["_jsonl_path"]
            items: list[Any] = []
            with open(jsonl_path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        items.append(json.loads(line))
            return {"items": items, "summary": output.get("summary", {})}
        return output

    def save_run_metadata(self, pipeline_name: str, status: str = "running", progress: dict = None) -> None:
        """Save run metadata for resume and live status polling.

        Credentials are intentionally excluded from the persisted JSON to
        prevent secrets from being written to disk (T-BRIX-V5-SEC-01).
        """
        import time
        meta = {
            "run_id": self.run_id,
            "pipeline": pipeline_name,
            "input": self.input,
            # NOTE: credentials are NOT persisted — they live only in RAM during execution
            "status": status,
            "completed_steps": list(self.step_outputs.keys()),
            "last_heartbeat": time.time(),
        }
        if progress is not None:
            meta["progress"] = progress
        meta_file = self.workdir / "run.json"
        meta_file.write_text(json.dumps(meta, default=str, indent=2))

    @classmethod
    def from_resume(cls, run_id: str) -> "PipelineContext":
        """Resume a previous run by loading workdir state."""
        workdir = WORKDIR_BASE / run_id
        if not workdir.exists():
            raise FileNotFoundError(f"Workdir not found: {workdir}")

        meta_file = workdir / "run.json"
        if not meta_file.exists():
            raise FileNotFoundError(f"No run.json in {workdir}")

        meta = json.loads(meta_file.read_text())

        ctx = cls(
            pipeline_input=meta.get("input", {}),
            workdir=workdir,
            resume_from=run_id,
        )
        ctx.run_id = run_id

        # Reload step outputs from persisted files.
        # JSONL files are loaded as lightweight references (not expanded into RAM).
        outputs_dir = workdir / "step_outputs"
        for output_file in outputs_dir.glob("*.json"):
            step_id = output_file.stem
            try:
                ctx.step_outputs[step_id] = json.loads(output_file.read_text())
            except (json.JSONDecodeError, ValueError):
                pass
        for jsonl_file in outputs_dir.glob("*.jsonl"):
            # Skip foreach checkpoint files — they are not step outputs
            if jsonl_file.stem.endswith("_checkpoint"):
                continue
            step_id = jsonl_file.stem
            if step_id not in ctx.step_outputs:
                # Reconstruct the lightweight RAM reference from the JSONL file
                count = sum(1 for line in open(jsonl_file) if line.strip())
                ctx.step_outputs[step_id] = {
                    "_large_output": True,
                    "_jsonl_path": str(jsonl_file),
                    "_count": count,
                    "summary": {},
                }

        return ctx

    def is_step_completed(self, step_id: str) -> bool:
        """Check if a step was already completed (for resume)."""
        if self._resume_from:
            return step_id in self.step_outputs
        return False

    # ------------------------------------------------------------------
    # foreach checkpoint helpers
    # ------------------------------------------------------------------

    def get_foreach_checkpoint_path(self, step_id: str) -> Path:
        """Path for foreach checkpoint JSONL file."""
        return self.workdir / "step_outputs" / f"{step_id}_checkpoint.jsonl"

    def write_foreach_checkpoint(self, step_id: str, item_index: int, item_input: Any, result: dict) -> None:
        """Write a single completed foreach item to checkpoint JSONL."""
        path = self.get_foreach_checkpoint_path(step_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        entry = {"index": item_index, "input": item_input, "result": result}
        with open(path, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")

    def load_foreach_checkpoint(self, step_id: str) -> dict:
        """Load completed items from checkpoint. Returns {index: result}."""
        path = self.get_foreach_checkpoint_path(step_id)
        if not path.exists():
            return {}
        completed: dict[int, dict] = {}
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                completed[entry["index"]] = entry["result"]
        return completed

    def save_file(self, filename: str, content: bytes) -> Path:
        """Save a file to the workdir and return the path."""
        file_path = self.workdir / "files" / filename
        file_path.write_bytes(content)
        return file_path

    def cleanup(self, keep: bool = False) -> None:
        """Remove workdir. Skip if keep=True."""
        if keep or not self.workdir.exists():
            return
        shutil.rmtree(self.workdir, ignore_errors=True)

    def to_jinja_context(self, item: Any = None) -> dict:
        """Build Jinja2 template context.

        Context contains:
        - input.*  — pipeline input parameters
        - credentials.* — resolved credential values
        - <step_id>.output — outputs from previous steps (wrapped in namespace)
        - <step_id>.items  — direct list shorthand for foreach outputs (INBOX-281)
        - item — current foreach item (if any)
        - var.* — managed variables from the variables table (T-BRIX-DB-13)
        - store.* — persistent store entries from the persistent_store table (T-BRIX-DB-13)

        For foreach step outputs (dicts with "items" list + "summary" keys) an
        additional ``items`` key is exposed on the step namespace so that
        downstream steps can use Jinja2 filters like ``selectattr`` and ``map``
        directly without having to navigate the wrapper dict:

            {{ process.items | selectattr("success") | list }}
            {{ process.items | map(attribute="data") | list }}

        ``output`` continues to hold the full wrapper dict for backward
        compatibility with existing pipelines and helper scripts.

        The base context (without item) is cached and only rebuilt when
        set_output is called. item-parameter calls never pollute the cache.
        """
        if self._jinja_cache is None:
            ctx: dict[str, Any] = {
                "input": self.input,
                "credentials": self.credentials,
            }
            # Load managed variables as var.* (T-BRIX-DB-13)
            # Secret variables are decrypted at runtime but their plaintext values
            # are tracked in _secret_values for redaction in step_executions (T-BRIX-DB-26).
            try:
                from brix.db import BrixDB
                from brix.credential_store import _decrypt
                db = BrixDB()
                var_dict: dict[str, str] = {}
                secret_values: set[str] = set()
                for entry in db.variable_list():
                    if entry.get("secret"):
                        # Decrypt the real value for Jinja2 context
                        raw = db.variable_get_raw(entry["name"])
                        if raw:
                            try:
                                plaintext = _decrypt(raw["value"])
                            except Exception:
                                plaintext = ""
                            var_dict[entry["name"]] = plaintext
                            if plaintext:
                                secret_values.add(plaintext)
                    else:
                        var_dict[entry["name"]] = entry["value"]
                ctx["var"] = var_dict
                ctx["_secret_values"] = secret_values
                self._secret_values = secret_values  # expose on context for engine redaction
                # Load persistent store as store.* (T-BRIX-DB-13)
                store_dict: dict[str, str] = {
                    entry["key"]: entry["value"]
                    for entry in db.store_list()
                }
                ctx["store"] = store_dict
            except Exception:
                ctx["var"] = {}
                ctx["store"] = {}
                ctx["_secret_values"] = set()
            # Add step outputs as step_id with .output accessor.
            # Large outputs are represented by a summary-only proxy so that Jinja2
            # templates never materialize the full items list into the context.
            for step_id, output in self.step_outputs.items():
                if isinstance(output, dict) and output.get("_large_output"):
                    ctx[step_id] = {
                        "output": {
                            "summary": output.get("summary", {}),
                            "count": output.get("_count", 0),
                            "_large": True,
                        }
                    }
                else:
                    step_ns: dict[str, Any] = {"output": output}
                    # INBOX-281: For foreach outputs expose a direct 'results' list so
                    # downstream Jinja2 filters (selectattr, map, …) work without
                    # having to navigate the {items, summary, success, duration} wrapper.
                    # Foreach outputs are dicts with an "items" list and a "summary" key.
                    # Note: 'items' cannot be used as the key name because Jinja2 resolves
                    # dict.items as Python's built-in method on a SandboxedEnvironment dict.
                    if (
                        isinstance(output, dict)
                        and isinstance(output.get("items"), list)
                        and "summary" in output
                    ):
                        step_ns["results"] = output["items"]
                    ctx[step_id] = step_ns
            self._jinja_cache = ctx

        if item is not None:
            # Merge item without polluting the cache
            return {**self._jinja_cache, "item": item}

        return self._jinja_cache
