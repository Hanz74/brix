"""REST API for pipeline execution — Cron, Webhooks, n8n, curl."""
import asyncio
import hmac
import json as _json
import logging
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import AsyncGenerator

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

from brix.pipeline_store import PipelineStore
from brix.engine import PipelineEngine
from brix.history import RunHistory
from brix.context import WORKDIR_BASE
from brix.config import config

API_KEY = os.environ.get("BRIX_API_KEY", "")
VERSION = "2.3.0"

logger = logging.getLogger(__name__)

if not API_KEY:
    logger.warning(
        "BRIX_API_KEY is not set — API access is restricted to localhost only. "
        "Set BRIX_API_KEY to enable remote access."
    )

# ---------------------------------------------------------------------------
# Idempotency store — SQLite-backed, 24-hour TTL
# ---------------------------------------------------------------------------

_IDEMPOTENCY_DB: Path = Path(os.environ.get("BRIX_IDEMPOTENCY_DB", "/tmp/brix_idempotency.db"))
_IDEMPOTENCY_TTL = config.IDEMPOTENCY_TTL


def _get_idempotency_conn() -> sqlite3.Connection:
    """Open (and initialise if needed) the idempotency SQLite database."""
    conn = sqlite3.connect(str(_IDEMPOTENCY_DB))
    conn.execute(
        "CREATE TABLE IF NOT EXISTS idempotency_keys "
        "(key TEXT PRIMARY KEY, run_id TEXT NOT NULL, created_at REAL NOT NULL)"
    )
    conn.commit()
    return conn


def _idempotency_lookup(key: str) -> str | None:
    """Return the existing run_id for *key* if it exists and has not expired."""
    conn = _get_idempotency_conn()
    try:
        cursor = conn.execute(
            "SELECT run_id FROM idempotency_keys WHERE key = ? AND created_at > ?",
            (key, time.time() - _IDEMPOTENCY_TTL),
        )
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _idempotency_store(key: str, run_id: str) -> None:
    """Persist *key → run_id* and purge expired entries."""
    conn = _get_idempotency_conn()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO idempotency_keys (key, run_id, created_at) VALUES (?, ?, ?)",
            (key, run_id, time.time()),
        )
        # Purge entries older than TTL
        conn.execute(
            "DELETE FROM idempotency_keys WHERE created_at <= ?",
            (time.time() - _IDEMPOTENCY_TTL,),
        )
        conn.commit()
    finally:
        conn.close()


def _is_localhost(request: Request) -> bool:
    """Return True if the request originates from loopback (127.x or ::1)."""
    client = request.client
    if client is None:
        return False
    host = client.host
    return host in ("127.0.0.1", "::1", "localhost")


def _check_auth(request: Request) -> bool:
    """Check API key authentication.

    When BRIX_API_KEY is set, the request must supply a matching X-API-Key header
    (compared with hmac.compare_digest to prevent timing attacks).

    When BRIX_API_KEY is NOT set, only localhost requests are permitted — remote
    callers receive 401 rather than open access.
    """
    if not API_KEY:
        # No key configured: allow only loopback addresses
        return _is_localhost(request)
    provided = request.headers.get("X-API-Key", "")
    return hmac.compare_digest(provided, API_KEY)


async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "version": VERSION})


async def list_pipelines(request: Request) -> JSONResponse:
    if not _check_auth(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    store = PipelineStore()
    return JSONResponse({"pipelines": store.list_all()})


async def run_pipeline(request: Request) -> JSONResponse:
    if not _check_auth(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    name = request.path_params["name"]

    try:
        body = await request.json()
    except Exception:
        body = {}

    store = PipelineStore()
    try:
        pipeline = store.load(name)
    except FileNotFoundError:
        return JSONResponse({"error": f"Pipeline '{name}' not found"}, status_code=404)

    async_mode = body.pop("async", False)

    # Warn about unknown input parameters (V2-20)
    defined_params = set(pipeline.input.keys())
    unknown_params = set(body.keys()) - defined_params
    warnings: list[str] = []
    if unknown_params:
        warnings.append(
            f"Unknown input parameters (ignored): {', '.join(sorted(unknown_params))}"
        )

    if async_mode:
        pre_run_id = f"run-{uuid.uuid4().hex[:12]}"

        async def _bg() -> None:
            engine = PipelineEngine()
            await engine.run(pipeline, body, run_id=pre_run_id)

        asyncio.create_task(_bg())
        return JSONResponse(
            {"run_id": pre_run_id, "status": "running", "warnings": warnings},
            status_code=202,
        )

    engine = PipelineEngine()
    result = await engine.run(pipeline, body)

    response_data = result.model_dump()
    response_data["warnings"] = warnings
    return JSONResponse(response_data, status_code=200 if result.success else 500)


async def get_run_status(request: Request) -> JSONResponse:
    import json as _json
    import time as _time

    if not _check_auth(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    run_id = request.path_params["run_id"]

    # 1. Try live run.json from workdir (running or recently finished)
    from brix.context import WORKDIR_BASE
    run_json_path = WORKDIR_BASE / run_id / "run.json"
    if run_json_path.exists():
        try:
            with open(run_json_path) as f:
                live = _json.load(f)
            if live.get("status") == "running":
                heartbeat = live.get("last_heartbeat", 0)
                age = _time.time() - heartbeat if heartbeat else 0
                live["suspected_hang"] = age > 300
                return JSONResponse({"source": "live", **live})
        except (OSError, ValueError):
            pass

    # 2. Fallback to SQLite history (completed runs)
    history = RunHistory()
    run = history.get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    return JSONResponse({"source": "history", **run})


async def approve_run(request: Request) -> JSONResponse:
    """Approve or reject a pending pipeline run (T-BRIX-V4-12)."""
    if not _check_auth(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    run_id = request.path_params["run_id"]
    try:
        body = await request.json()
    except Exception:
        body = {}

    import json as _json
    approval_file = WORKDIR_BASE / run_id / "approval_pending.json"
    if not approval_file.exists():
        return JSONResponse({"error": "No pending approval"}, status_code=404)

    data = _json.loads(approval_file.read_text())
    data["status"] = body.get("action", "approved")   # "approved" or "rejected"
    data["approved_by"] = body.get("by", "api")
    data["reason"] = body.get("reason", "")
    approval_file.write_text(_json.dumps(data))

    return JSONResponse({"success": True, "run_id": run_id, "status": data["status"]})


async def webhook(request: Request) -> JSONResponse:
    """Webhook trigger — pipeline name in URL, body as params.

    Enhancements (T-BRIX-V5-06):
    * Async mode: starts pipeline in background, returns run_id immediately.
    * Auth: BRIX_API_KEY accepted as fallback when no per-pipeline secret is set.
    * Payload validation: validates body against pipeline input_schema when present.
    * Idempotency: X-Idempotency-Key header deduplicates requests within 24 h.
    """
    name = request.path_params["name"]

    # Normalize name: hyphens → underscores for env var compatibility
    env_name = name.upper().replace("-", "_")
    webhook_secret = os.environ.get(f"BRIX_WEBHOOK_SECRET_{env_name}", "")

    if webhook_secret:
        # Per-pipeline secret takes precedence — must match exactly
        provided_secret = request.headers.get("X-Webhook-Secret", "")
        if not hmac.compare_digest(provided_secret, webhook_secret):
            # Fallback: also accept BRIX_API_KEY as alternative auth
            if not (API_KEY and hmac.compare_digest(
                request.headers.get("X-API-Key", ""), API_KEY
            )):
                return JSONResponse({"error": "Invalid webhook secret"}, status_code=403)
    else:
        # No per-pipeline secret: use global auth check
        if not _check_auth(request):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)

    # ------------------------------------------------------------------
    # Idempotency check
    # ------------------------------------------------------------------
    idempotency_key = request.headers.get("X-Idempotency-Key", "")
    if idempotency_key:
        existing_run_id = _idempotency_lookup(idempotency_key)
        if existing_run_id:
            return JSONResponse(
                {"run_id": existing_run_id, "status": "duplicate"},
                status_code=200,
            )

    try:
        body = await request.json()
    except Exception:
        body = {}

    store = PipelineStore()
    try:
        pipeline = store.load(name)
    except FileNotFoundError:
        return JSONResponse({"error": f"Pipeline '{name}' not found"}, status_code=404)

    # ------------------------------------------------------------------
    # Payload validation against pipeline input schema
    # ------------------------------------------------------------------
    if pipeline.input:
        validation_errors: list[str] = []
        for param_name, param_def in pipeline.input.items():
            if param_def.default is None and param_name not in body:
                validation_errors.append(f"Missing required parameter: '{param_name}'")
        if validation_errors:
            return JSONResponse(
                {"error": "Payload validation failed", "details": validation_errors},
                status_code=400,
            )

    # ------------------------------------------------------------------
    # Async mode — start pipeline in background, return run_id immediately
    # ------------------------------------------------------------------
    pre_run_id = f"run-{uuid.uuid4().hex[:12]}"

    # Persist idempotency key before spawning so duplicate requests during
    # startup also get deduplicated.
    if idempotency_key:
        _idempotency_store(idempotency_key, pre_run_id)

    async def _bg() -> None:
        engine = PipelineEngine()
        await engine.run(pipeline, body, run_id=pre_run_id)

    asyncio.create_task(_bg())

    return JSONResponse(
        {"run_id": pre_run_id, "status": "started"},
        status_code=202,
    )


_SSE_POLL_INTERVAL = config.SSE_POLL_INTERVAL
_SSE_TIMEOUT = config.SSE_TIMEOUT

_TERMINAL_STATUSES = frozenset({"success", "completed", "failed", "error", "cancelled"})


def _sse_event(event: str, data: dict) -> str:
    """Format a single Server-Sent Events message."""
    payload = _json.dumps(data)
    return f"event: {event}\ndata: {payload}\n\n"


async def _stream_run_events(run_id: str) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted strings until the run finishes."""
    workdir = WORKDIR_BASE / run_id
    run_json_path = workdir / "run.json"
    sp_path = workdir / "step_progress.json"

    # Wait up to 5 s for the workdir to appear (async run may not have started yet)
    deadline = time.monotonic() + 5.0
    while not workdir.exists() and time.monotonic() < deadline:
        await asyncio.sleep(0.2)

    if not workdir.exists():
        yield _sse_event("error", {"error": "Run not found", "run_id": run_id})
        return

    # Emit initial connected event
    yield _sse_event("connected", {"run_id": run_id})

    last_progress_snapshot: str = ""
    timeout_at = time.monotonic() + _SSE_TIMEOUT

    while time.monotonic() < timeout_at:
        # --- read run.json ---
        run_data: dict = {}
        try:
            if run_json_path.exists():
                run_data = _json.loads(run_json_path.read_text())
        except (OSError, ValueError):
            pass

        status = run_data.get("status", "running")

        # --- read step_progress.json ---
        sp_data: dict = {}
        try:
            if sp_path.exists():
                sp_data = _json.loads(sp_path.read_text())
        except (OSError, ValueError):
            pass

        # Only emit a progress event when the snapshot has changed
        sp_snapshot = _json.dumps(sp_data, sort_keys=True)
        if sp_data and sp_snapshot != last_progress_snapshot:
            last_progress_snapshot = sp_snapshot
            yield _sse_event("progress", {"run_id": run_id, "step_progress": sp_data})

        # Emit status event on every tick so the client can track heartbeats
        yield _sse_event("status", {"run_id": run_id, "status": status, **run_data})

        if status in _TERMINAL_STATUSES:
            yield _sse_event("done", {"run_id": run_id, "status": status})
            return

        await asyncio.sleep(_SSE_POLL_INTERVAL)

    # Timeout reached
    yield _sse_event("timeout", {"run_id": run_id, "error": "SSE stream timed out"})


async def stream_run(request: Request) -> StreamingResponse:
    """GET /stream/{run_id} — Server-Sent Events stream for a pipeline run.

    Emits the following event types:
    - connected  — immediately on connection, confirms run_id
    - progress   — when step_progress.json changes (foreach/batch progress)
    - status     — on every poll tick with current run state from run.json
    - done       — when the run reaches a terminal status
    - error      — run not found or other fatal conditions
    - timeout    — SSE_TIMEOUT exceeded without terminal status
    """
    if not _check_auth(request):
        # SSE cannot return JSON 401 mid-stream; send a single error event then close.
        async def _unauth() -> AsyncGenerator[str, None]:
            yield _sse_event("error", {"error": "Unauthorized"})
        return StreamingResponse(
            _unauth(),
            status_code=401,
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    run_id = request.path_params["run_id"]
    return StreamingResponse(
        _stream_run_events(run_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


app = Starlette(
    routes=[
        Route("/health", health),
        Route("/pipelines", list_pipelines),
        Route("/run/{name}", run_pipeline, methods=["POST"]),
        Route("/status/{run_id}", get_run_status),
        Route("/stream/{run_id}", stream_run),
        Route("/approve/{run_id}", approve_run, methods=["POST"]),
        Route("/webhook/{name}", webhook, methods=["POST"]),
    ]
)
