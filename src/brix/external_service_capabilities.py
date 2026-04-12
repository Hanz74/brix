"""External service capability and version handshakes."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

import httpx

from brix.config import BrixConfig


def _join_url(base_url: str, endpoint: str) -> str:
    normalized_endpoint = str(endpoint or "").strip()
    if not normalized_endpoint.startswith("/"):
        normalized_endpoint = f"/{normalized_endpoint}"
    return f"{base_url.rstrip('/')}{normalized_endpoint}"


@dataclass(frozen=True)
class ExternalServiceCapabilities:
    service: str
    version: str | None
    supports_async_jobs: bool
    supports_job_status: bool
    supports_job_result: bool
    job_progress_fields: tuple[str, ...]
    drift_issues: tuple[str, ...]
    raw_health: dict[str, Any]
    raw_tips: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


async def _fetch_json(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    response = await client.get(url)
    if getattr(response, "is_error", False):
        return {}
    try:
        payload = response.json()
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


async def fetch_daigestr_capabilities(
    *,
    base_url: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> ExternalServiceCapabilities:
    cfg = BrixConfig.reload()
    resolved_base_url = (base_url or cfg.DAIGESTR_URL).rstrip("/")
    own_client = client is None
    http_client = client or httpx.AsyncClient(timeout=cfg.BRIX_DEFAULT_TIMEOUT)
    try:
        health = await _fetch_json(http_client, _join_url(resolved_base_url, cfg.DAIGESTR_HEALTH_ENDPOINT))
        tips = await _fetch_json(http_client, _join_url(resolved_base_url, cfg.DAIGESTR_TIPS_ENDPOINT))
    finally:
        if own_client:
            await http_client.aclose()

    response_contract = tips.get("response_contract") if isinstance(tips.get("response_contract"), dict) else {}
    job_progress_endpoints = (
        response_contract.get("job_progress_endpoints")
        if isinstance(response_contract.get("job_progress_endpoints"), dict)
        else {}
    )
    job_progress_fields_raw = response_contract.get("job_progress_fields")
    if isinstance(job_progress_fields_raw, dict):
        job_progress_fields = tuple(sorted(job_progress_fields_raw.keys()))
    elif isinstance(job_progress_fields_raw, list):
        job_progress_fields = tuple(str(item) for item in job_progress_fields_raw)
    else:
        job_progress_fields = ()

    supports_job_start = "start" in job_progress_endpoints
    supports_job_status = "status" in job_progress_endpoints
    supports_job_result = "result" in job_progress_endpoints
    supports_async_jobs = supports_job_start and supports_job_status and supports_job_result
    version = str(health.get("version") or "").strip() or None
    drift_issues: list[str] = []
    if version is None:
        drift_issues.append("missing_service_version")
    if supports_job_start and not supports_async_jobs:
        drift_issues.append("async_contract_incomplete")
    required_progress_fields = {"progress.status", "progress.job_id", "progress.current_stage"}
    if supports_job_status and not required_progress_fields.issubset(set(job_progress_fields)):
        drift_issues.append("job_progress_fields_incomplete")

    return ExternalServiceCapabilities(
        service="daigestr",
        version=version,
        supports_async_jobs=supports_async_jobs,
        supports_job_status=supports_job_status,
        supports_job_result=supports_job_result,
        job_progress_fields=job_progress_fields,
        drift_issues=tuple(drift_issues),
        raw_health=health,
        raw_tips=tips,
    )
