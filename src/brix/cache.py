"""MCP tool schema caching."""
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

# Default cache directory
CACHE_DIR = Path.home() / ".brix" / "cache"
DEFAULT_TTL_DAYS = 7


class SchemaCache:
    """Local cache for MCP tool schemas."""

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or CACHE_DIR

    def get_server_cache_dir(self, server_name: str) -> Path:
        """Get the cache directory for a server."""
        return self.cache_dir / server_name

    def save_tools(self, server_name: str, tools: list[dict]):
        """Save tool schemas to cache."""
        server_dir = self.get_server_cache_dir(server_name)
        server_dir.mkdir(parents=True, exist_ok=True)

        # Save tools
        tools_path = server_dir / "tools.json"
        tools_path.write_text(json.dumps(tools, indent=2))

        # Save metadata
        schema_hash = hashlib.sha256(json.dumps(tools, sort_keys=True).encode()).hexdigest()[:16]
        meta = {
            "cached_at": datetime.utcnow().isoformat(),
            "schema_hash": schema_hash,
            "ttl_days": DEFAULT_TTL_DAYS,
            "tool_count": len(tools),
        }
        meta_path = server_dir / "meta.json"
        meta_path.write_text(json.dumps(meta, indent=2))

    def load_tools(self, server_name: str) -> Optional[list[dict]]:
        """Load cached tool schemas. Returns None if not cached."""
        tools_path = self.get_server_cache_dir(server_name) / "tools.json"
        if not tools_path.exists():
            return None
        return json.loads(tools_path.read_text())

    def load_meta(self, server_name: str) -> Optional[dict]:
        """Load cache metadata."""
        meta_path = self.get_server_cache_dir(server_name) / "meta.json"
        if not meta_path.exists():
            return None
        return json.loads(meta_path.read_text())

    def is_valid(self, server_name: str) -> bool:
        """Check if cache is still valid (not expired)."""
        meta = self.load_meta(server_name)
        if meta is None:
            return False

        cached_at = datetime.fromisoformat(meta["cached_at"])
        ttl_days = meta.get("ttl_days", DEFAULT_TTL_DAYS)
        age_days = (datetime.utcnow() - cached_at).days

        return age_days < ttl_days

    def is_stale(self, server_name: str, current_hash: str) -> bool:
        """Check if cache hash differs from current schema."""
        meta = self.load_meta(server_name)
        if meta is None:
            return True
        return meta.get("schema_hash") != current_hash

    def compute_hash(self, tools: list[dict]) -> str:
        """Compute schema hash for comparison."""
        return hashlib.sha256(json.dumps(tools, sort_keys=True).encode()).hexdigest()[:16]

    def invalidate(self, server_name: str):
        """Remove cached data for a server."""
        server_dir = self.get_server_cache_dir(server_name)
        if server_dir.exists():
            for f in server_dir.iterdir():
                f.unlink()
            server_dir.rmdir()

    def get_tool_names(self, server_name: str) -> list[str]:
        """Get list of cached tool names."""
        tools = self.load_tools(server_name)
        if tools is None:
            return []
        return [t.get("name", "") for t in tools]

    def get_tool_schema(self, server_name: str, tool_name: str) -> Optional[dict]:
        """Get schema for a specific tool."""
        tools = self.load_tools(server_name)
        if tools is None:
            return None
        for tool in tools:
            if tool.get("name") == tool_name:
                return tool
        return None

    def list_cached_servers(self) -> list[str]:
        """List all servers with cached schemas."""
        if not self.cache_dir.exists():
            return []
        return [d.name for d in self.cache_dir.iterdir() if d.is_dir() and (d / "tools.json").exists()]
