"""Credential Store — Fernet-encrypted secrets with UUID references.

Stores credentials in a dedicated SQLite database (~/.brix/credentials.db),
separate from the main brix.db. Values are never exposed via list/get —
only via resolve() which is used internally by the pipeline engine.

Encryption key: BRIX_MASTER_KEY env var (hex-encoded 32-byte key).
If unset, a per-host default key is derived and a warning is emitted.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import uuid
import warnings
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Literal, Optional

logger = logging.getLogger(__name__)

CREDENTIAL_TYPES = ("api-key", "oauth2", "basic-auth")
CredentialType = Literal["api-key", "oauth2", "basic-auth"]

DEFAULT_DB_PATH = Path.home() / ".brix" / "credentials.db"

# UUID prefix used to distinguish credential UUIDs from plain env-var refs
CRED_UUID_PREFIX = "cred-"


def _get_fernet():
    """Return a Fernet instance using BRIX_MASTER_KEY (or a derived default)."""
    try:
        from cryptography.fernet import Fernet
        import base64
        import hashlib
    except ImportError as exc:
        raise RuntimeError(
            "cryptography package is required for CredentialStore. "
            "Install with: pip install cryptography"
        ) from exc

    raw_key = os.environ.get("BRIX_MASTER_KEY", "")
    if raw_key:
        # Accept hex-encoded 32-byte key or raw 32-byte key
        try:
            key_bytes = bytes.fromhex(raw_key)
            if len(key_bytes) != 32:
                raise ValueError("BRIX_MASTER_KEY must be 32 bytes (64 hex chars)")
            fernet_key = base64.urlsafe_b64encode(key_bytes)
        except ValueError:
            # Try treating as raw bytes if exactly 32 chars
            if len(raw_key) == 32:
                fernet_key = base64.urlsafe_b64encode(raw_key.encode())
            else:
                raise ValueError(
                    "BRIX_MASTER_KEY must be a 64-character hex string (32 bytes) "
                    "or a 32-character string"
                )
    else:
        # Derive a stable per-host key from the hostname + a fixed salt.
        # This provides basic obfuscation but warns the user.
        import socket
        warnings.warn(
            "BRIX_MASTER_KEY is not set. Using a derived default key — "
            "credentials are NOT securely encrypted. "
            "Set BRIX_MASTER_KEY to a 64-char hex string for real security. "
            "Generate one with: python3 -c \"import secrets; print(secrets.token_hex(32))\"",
            UserWarning,
            stacklevel=3,
        )
        hostname = socket.gethostname()
        salt = b"brix-credential-store-default-salt-v1"
        derived = hashlib.pbkdf2_hmac("sha256", hostname.encode(), salt, iterations=100_000)
        fernet_key = base64.urlsafe_b64encode(derived)

    return Fernet(fernet_key)


def _encrypt(value: str) -> str:
    """Encrypt a plaintext value and return base64-encoded ciphertext."""
    fernet = _get_fernet()
    return fernet.encrypt(value.encode()).decode()


def _decrypt(ciphertext: str) -> str:
    """Decrypt a Fernet ciphertext and return the plaintext value."""
    fernet = _get_fernet()
    return fernet.decrypt(ciphertext.encode()).decode()


class CredentialNotFoundError(KeyError):
    """Raised when a credential cannot be found by id or name."""
    pass


class CredentialStore:
    """Manages encrypted credentials in ~/.brix/credentials.db.

    Public API (never exposes plaintext values):
        add(name, type, value)    → credential_id (UUID string)
        get(id_or_name)           → metadata dict (no value)
        list()                    → list of metadata dicts (no values)
        update(id_or_name, ...)   → updates name and/or re-encrypts new value
        delete(id_or_name)        → True if deleted

    Internal API (for pipeline engine only):
        resolve(id_or_name)       → plaintext value
    """

    def __init__(self, db_path: Path | str | None = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _connect(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS credentials (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    type TEXT NOT NULL,
                    encrypted_value TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _resolve_row(self, id_or_name: str) -> sqlite3.Row | None:
        """Return the raw DB row for a credential by UUID or name."""
        with self._connect() as conn:
            # Try by UUID first (exact match)
            row = conn.execute(
                "SELECT * FROM credentials WHERE id = ?", (id_or_name,)
            ).fetchone()
            if row is None:
                # Try by name
                row = conn.execute(
                    "SELECT * FROM credentials WHERE name = ?", (id_or_name,)
                ).fetchone()
        return row

    # ------------------------------------------------------------------
    # Public CRUD (no values returned)
    # ------------------------------------------------------------------

    def add(self, name: str, cred_type: CredentialType, value: str) -> str:
        """Add a new credential. Returns the UUID string.

        Raises ValueError if the type is invalid.
        Raises sqlite3.IntegrityError if name already exists.
        """
        if cred_type not in CREDENTIAL_TYPES:
            raise ValueError(
                f"Invalid credential type '{cred_type}'. "
                f"Must be one of: {', '.join(CREDENTIAL_TYPES)}"
            )
        cred_id = str(uuid.uuid4())
        encrypted = _encrypt(value)
        now = self._now_iso()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO credentials (id, name, type, encrypted_value, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (cred_id, name, cred_type, encrypted, now, now),
            )
        return cred_id

    def get(self, id_or_name: str) -> dict:
        """Return credential metadata (without value).

        Raises CredentialNotFoundError if not found.
        """
        row = self._resolve_row(id_or_name)
        if row is None:
            raise CredentialNotFoundError(
                f"Credential not found: '{id_or_name}'"
            )
        return {
            "id": row["id"],
            "name": row["name"],
            "type": row["type"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list(self) -> list[dict]:
        """Return metadata for all credentials (without values), sorted by name."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, name, type, created_at, updated_at "
                "FROM credentials ORDER BY name"
            ).fetchall()
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "type": r["type"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
            }
            for r in rows
        ]

    def update(
        self,
        id_or_name: str,
        *,
        value: str | None = None,
        name: str | None = None,
    ) -> dict:
        """Update a credential's name and/or value. Returns updated metadata.

        Raises CredentialNotFoundError if not found.
        """
        row = self._resolve_row(id_or_name)
        if row is None:
            raise CredentialNotFoundError(
                f"Credential not found: '{id_or_name}'"
            )
        cred_id = row["id"]
        now = self._now_iso()

        if value is not None:
            encrypted = _encrypt(value)
        else:
            encrypted = row["encrypted_value"]

        new_name = name if name is not None else row["name"]

        with self._connect() as conn:
            conn.execute(
                "UPDATE credentials SET name = ?, encrypted_value = ?, updated_at = ? "
                "WHERE id = ?",
                (new_name, encrypted, now, cred_id),
            )
        return self.get(cred_id)

    def delete(self, id_or_name: str) -> bool:
        """Delete a credential. Returns True if deleted, False if not found."""
        row = self._resolve_row(id_or_name)
        if row is None:
            return False
        with self._connect() as conn:
            conn.execute("DELETE FROM credentials WHERE id = ?", (row["id"],))
        return True

    def search(self, query: str) -> list[dict]:
        """Search credentials by name or type substring match.

        Returns a list of metadata dicts (no values), sorted by name.
        The search is case-insensitive.
        """
        q = query.lower()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, name, type, created_at, updated_at FROM credentials ORDER BY name"
            ).fetchall()
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "type": r["type"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
            }
            for r in rows
            if q in r["name"].lower() or q in r["type"].lower()
        ]

    def rotate(self, id_or_name: str) -> dict:
        """Rotate an OAuth2 credential using its refresh_token to get a new access_token.

        For non-OAuth2 credentials raises ``ValueError`` with a hint.
        For OAuth2 credentials the value must be a JSON dict containing at least
        ``refresh_token`` and ``token_url`` fields.  The method POSTs to
        ``token_url`` and stores the new ``access_token`` (merged back into the
        stored value dict).

        Returns updated metadata dict (no value).
        Raises ``CredentialNotFoundError`` if not found.
        Raises ``ValueError`` if not an OAuth2 credential or refresh fails.
        """
        row = self._resolve_row(id_or_name)
        if row is None:
            raise CredentialNotFoundError(f"Credential not found: '{id_or_name}'")

        cred_id = row["id"]
        cred_type = row["type"]

        if cred_type != "oauth2":
            raise ValueError(
                f"Credential '{row['name']}' is of type '{cred_type}', not 'oauth2'. "
                "Only oauth2 credentials can be rotated. "
                "For api-key / basic-auth, use brix__credential_update to set a new value."
            )

        # Decrypt current value — expected to be a JSON dict
        current_value = _decrypt(row["encrypted_value"])
        try:
            cred_data: dict = json.loads(current_value)
        except (json.JSONDecodeError, ValueError) as exc:
            raise ValueError(
                f"oauth2 credential '{row['name']}' value is not valid JSON: {exc}. "
                "Expected JSON with keys: refresh_token, token_url (and optionally client_id, client_secret)."
            ) from exc

        refresh_token = cred_data.get("refresh_token")
        token_url = cred_data.get("token_url")

        if not refresh_token:
            raise ValueError(
                f"oauth2 credential '{row['name']}' has no 'refresh_token' field. "
                "Cannot rotate without a refresh token."
            )
        if not token_url:
            raise ValueError(
                f"oauth2 credential '{row['name']}' has no 'token_url' field. "
                "Cannot rotate without knowing the token endpoint."
            )

        # Perform token refresh
        import urllib.request
        import urllib.parse

        post_data: dict = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        if cred_data.get("client_id"):
            post_data["client_id"] = cred_data["client_id"]
        if cred_data.get("client_secret"):
            post_data["client_secret"] = cred_data["client_secret"]

        encoded = urllib.parse.urlencode(post_data).encode()
        req = urllib.request.Request(
            token_url,
            data=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                token_response = json.loads(resp.read())
        except Exception as exc:
            raise ValueError(
                f"Token refresh request to '{token_url}' failed: {exc}"
            ) from exc

        new_access_token = token_response.get("access_token")
        if not new_access_token:
            raise ValueError(
                f"Token refresh response did not contain 'access_token': {token_response}"
            )

        # Merge new tokens into stored value
        cred_data["access_token"] = new_access_token
        if "refresh_token" in token_response:
            cred_data["refresh_token"] = token_response["refresh_token"]
        if "expires_in" in token_response:
            cred_data["expires_in"] = token_response["expires_in"]

        # Re-encrypt and update
        new_encrypted = _encrypt(json.dumps(cred_data))
        now = self._now_iso()
        with self._connect() as conn:
            conn.execute(
                "UPDATE credentials SET encrypted_value = ?, updated_at = ? WHERE id = ?",
                (new_encrypted, now, cred_id),
            )

        return self.get(cred_id)

    # ------------------------------------------------------------------
    # Internal API — exposes plaintext value
    # ------------------------------------------------------------------

    def resolve(self, id_or_name: str) -> str:
        """Decrypt and return the plaintext credential value.

        For internal use by the pipeline engine ONLY.
        Value is NEVER stored in run.json, step_output, or logs.

        Raises CredentialNotFoundError if not found.
        """
        row = self._resolve_row(id_or_name)
        if row is None:
            raise CredentialNotFoundError(
                f"Credential not found: '{id_or_name}'"
            )
        return _decrypt(row["encrypted_value"])


def is_credential_uuid(value: str) -> bool:
    """Return True if value looks like a brix credential UUID reference.

    Accepts both prefixed form ('cred-<uuid>') and raw UUID strings.
    """
    if value.startswith(CRED_UUID_PREFIX):
        candidate = value[len(CRED_UUID_PREFIX):]
    else:
        candidate = value
    try:
        uuid.UUID(candidate)
        return True
    except (ValueError, AttributeError):
        return False
