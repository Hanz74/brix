"""Credential store handler module."""
from __future__ import annotations

from brix.credential_store import CredentialStore, CredentialNotFoundError, CREDENTIAL_TYPES


async def _handle_credential_add(arguments: dict) -> dict:
    """Add a new encrypted credential."""
    import sqlite3 as _sqlite3

    name = arguments.get("name", "").strip()
    cred_type = arguments.get("type", "").strip()
    value = arguments.get("value", "")

    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}
    if not cred_type:
        return {"success": False, "error": "Parameter 'type' is required"}
    if cred_type not in CREDENTIAL_TYPES:
        return {
            "success": False,
            "error": f"Invalid type '{cred_type}'. Must be one of: {', '.join(CREDENTIAL_TYPES)}",
        }
    if not value:
        return {"success": False, "error": "Parameter 'value' is required"}

    try:
        store = CredentialStore()
        cred_id = store.add(name, cred_type, value)
        meta = store.get(cred_id)
        return {
            "success": True,
            "id": cred_id,
            "name": meta["name"],
            "type": meta["type"],
            "created_at": meta["created_at"],
            "note": "Value is encrypted and will NOT be shown. Use this UUID in pipeline credentials.",
        }
    except _sqlite3.IntegrityError:
        return {
            "success": False,
            "error": f"A credential named '{name}' already exists. Use brix__credential_update to change it.",
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_credential_list(arguments: dict) -> dict:
    """List all credentials (metadata only — no values)."""
    try:
        store = CredentialStore()
        items = store.list()
        return {
            "success": True,
            "count": len(items),
            "credentials": items,
            "note": "Values are encrypted and never shown. Use UUIDs in pipeline credentials.",
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_credential_get(arguments: dict) -> dict:
    """Get metadata for a single credential (no value)."""
    id_or_name = arguments.get("id_or_name", "").strip()
    if not id_or_name:
        return {"success": False, "error": "Parameter 'id_or_name' is required"}

    try:
        store = CredentialStore()
        meta = store.get(id_or_name)
        return {"success": True, **meta}
    except CredentialNotFoundError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_credential_update(arguments: dict) -> dict:
    """Update a credential's name and/or value."""
    id_or_name = arguments.get("id_or_name", "").strip()
    value = arguments.get("value", None)
    name = arguments.get("name", None)

    if not id_or_name:
        return {"success": False, "error": "Parameter 'id_or_name' is required"}
    if value is None and name is None:
        return {"success": False, "error": "At least one of 'value' or 'name' must be provided"}

    try:
        store = CredentialStore()
        meta = store.update(id_or_name, value=value, name=name)
        return {
            "success": True,
            **meta,
            "note": "Value updated (encrypted). It will NOT be shown.",
        }
    except CredentialNotFoundError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_credential_delete(arguments: dict) -> dict:
    """Delete a credential."""
    id_or_name = arguments.get("id_or_name", "").strip()
    if not id_or_name:
        return {"success": False, "error": "Parameter 'id_or_name' is required"}

    try:
        store = CredentialStore()
        deleted = store.delete(id_or_name)
        if deleted:
            return {"success": True, "deleted": id_or_name}
        return {"success": False, "error": f"Credential '{id_or_name}' not found"}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_credential_rotate(arguments: dict) -> dict:
    """Rotate an OAuth2 credential via refresh_token."""
    id_or_name = arguments.get("id", "").strip()
    if not id_or_name:
        return {"success": False, "error": "Parameter 'id' is required."}

    try:
        store = CredentialStore()
        meta = store.rotate(id_or_name)
        return {
            "success": True,
            **meta,
            "note": "access_token rotated via refresh_token. New value is encrypted and NOT shown.",
        }
    except CredentialNotFoundError as exc:
        return {"success": False, "error": str(exc)}
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_credential_search(arguments: dict) -> dict:
    """Search credentials by name or type substring."""
    query = arguments.get("query", "").strip()
    if not query:
        return {"success": False, "error": "Parameter 'query' is required."}

    try:
        store = CredentialStore()
        results = store.search(query)
        return {
            "success": True,
            "query": query,
            "credentials": results,
            "total": len(results),
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}
