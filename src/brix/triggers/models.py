"""Trigger configuration models."""
from pydantic import BaseModel
from typing import Any, Optional

from brix.config import config


class TriggerConfig(BaseModel):
    id: str
    type: str  # "mail", "file", "http_poll", "pipeline_done"
    interval: str = config.TRIGGER_DEFAULT_INTERVAL  # Polling interval
    pipeline: str  # Pipeline name to run
    params: dict[str, Any] = {}  # Template params ({{ trigger.* }})
    dedupe_key: str = ""  # Jinja2 expression for dedup
    enabled: bool = True
    # Type-specific fields
    filter: dict[str, Any] = {}
    path: Optional[str] = None
    pattern: Optional[str] = None
    url: Optional[str] = None
    headers: dict[str, str] = {}
    hash_field: Optional[str] = None
    status: Optional[str] = None  # For pipeline_done: success/failure/any
    pipeline_target: Optional[str] = None  # Alias for pipeline (pipeline_done)
    # T-BRIX-V6-20: Intelligent Pipeline Chaining
    input_filter: dict[str, Any] = {}  # Only fire if source run had these input params
    when: str = ""  # Jinja2 expression evaluated against trigger.source_run.result
    forward_input: dict[str, Any] = {}  # Jinja2 exprs mapped to triggered pipeline input
    pipelines: list[dict[str, Any]] = []  # Multi-pipeline targets: [{pipeline, params}, ...]
    # T-BRIX-V6-BUG-02: Mail provider selection
    provider: str = "m365"  # "m365" (default) or "imap"
    # IMAP-specific fields (used when provider == "imap")
    email: Optional[str] = None  # IMAP login email address
    app_password_credential: Optional[str] = None  # UUID from CredentialStore
    folder: str = "INBOX"  # IMAP folder/mailbox to monitor
    server: str = config.IMAP_DEFAULT_SERVER  # IMAP server hostname
    # T-BRIX-DB-22: Debounce — wait for quiet period before firing pipeline
    # e.g. "5m" means: wait 5 minutes after last event before firing.
    # If more events arrive within the window the timer resets.
    debounce: Optional[str] = None  # Duration string, e.g. "5m", "30s"
