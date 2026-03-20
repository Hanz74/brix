"""REST API for pipeline execution — Cron, Webhooks, n8n, curl."""
import os

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from brix.pipeline_store import PipelineStore
from brix.engine import PipelineEngine
from brix.history import RunHistory

API_KEY = os.environ.get("BRIX_API_KEY", "")
VERSION = "2.3.0"


def _check_auth(request: Request) -> bool:
    """Check API key if configured."""
    if not API_KEY:
        return True  # No auth configured
    return request.headers.get("X-API-Key") == API_KEY


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

    engine = PipelineEngine()
    result = await engine.run(pipeline, body)

    return JSONResponse(result.model_dump(), status_code=200 if result.success else 500)


async def get_run_status(request: Request) -> JSONResponse:
    if not _check_auth(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    run_id = request.path_params["run_id"]
    history = RunHistory()
    run = history.get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    return JSONResponse(run)


async def webhook(request: Request) -> JSONResponse:
    """Webhook trigger — pipeline name in URL, body as params."""
    if not _check_auth(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    name = request.path_params["name"]

    # Optional per-pipeline webhook secret validation
    # Normalize name: hyphens → underscores for env var compatibility
    env_name = name.upper().replace("-", "_")
    webhook_secret = os.environ.get(f"BRIX_WEBHOOK_SECRET_{env_name}", "")
    if webhook_secret:
        if request.headers.get("X-Webhook-Secret") != webhook_secret:
            return JSONResponse({"error": "Invalid webhook secret"}, status_code=403)

    try:
        body = await request.json()
    except Exception:
        body = {}

    store = PipelineStore()
    try:
        pipeline = store.load(name)
    except FileNotFoundError:
        return JSONResponse({"error": f"Pipeline '{name}' not found"}, status_code=404)

    engine = PipelineEngine()
    result = await engine.run(pipeline, body)

    return JSONResponse(result.model_dump(), status_code=200 if result.success else 500)


app = Starlette(
    routes=[
        Route("/health", health),
        Route("/pipelines", list_pipelines),
        Route("/run/{name}", run_pipeline, methods=["POST"]),
        Route("/status/{run_id}", get_run_status),
        Route("/webhook/{name}", webhook, methods=["POST"]),
    ]
)
