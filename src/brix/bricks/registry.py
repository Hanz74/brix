"""Brick registry: built-in bricks + MCP auto-discovery."""
from typing import Optional

from brix.bricks.schema import BrickParam, BrickSchema
from brix.bricks.builtins import ALL_BUILTINS
from brix.cache import SchemaCache


class BrickRegistry:
    """Central registry of all available bricks."""

    def __init__(self):
        self._bricks: dict[str, BrickSchema] = {}
        self._load_builtins()

    def _load_builtins(self):
        """Register all built-in bricks."""
        for brick in ALL_BUILTINS:
            self._bricks[brick.name] = brick

    def register(self, brick: BrickSchema):
        """Register a custom brick."""
        self._bricks[brick.name] = brick

    def unregister(self, name: str):
        """Remove a brick from registry."""
        self._bricks.pop(name, None)

    def get(self, name: str) -> Optional[BrickSchema]:
        """Get a brick by name."""
        return self._bricks.get(name)

    def list_all(self) -> list[BrickSchema]:
        """List all registered bricks."""
        return list(self._bricks.values())

    def list_by_category(self, category: str) -> list[BrickSchema]:
        """List bricks filtered by category."""
        return [b for b in self._bricks.values() if b.category == category]

    def search(self, query: str, category: str = None) -> list[BrickSchema]:
        """Search bricks by keyword in name, description, when_to_use.

        Optional category filter. Case-insensitive.
        """
        query_lower = query.lower()
        results = []
        for brick in self._bricks.values():
            if category and brick.category != category:
                continue
            searchable = f"{brick.name} {brick.description} {brick.when_to_use}".lower()
            if query_lower in searchable:
                results.append(brick)
        return results

    def get_categories(self) -> list[str]:
        """Get all unique categories."""
        return sorted(set(b.category for b in self._bricks.values()))

    def discover_mcp_bricks(self, server_name: str, cache: SchemaCache = None) -> int:
        """Auto-discover bricks from a cached MCP server's tool list.

        Each MCP tool becomes a brick with type='mcp'.

        Returns:
            Number of bricks discovered and registered.
        """
        _cache = cache or SchemaCache()
        tools = _cache.load_tools(server_name)
        if not tools:
            return 0

        count = 0
        for tool in tools:
            tool_name = tool.get("name", "")
            if not tool_name:
                continue

            # Build brick from MCP tool schema
            brick_name = f"{server_name}:{tool_name}"

            # Convert MCP inputSchema to BrickParams
            config_params: dict[str, BrickParam] = {}
            input_schema = tool.get("inputSchema", {})
            properties = input_schema.get("properties", {})
            required_fields = input_schema.get("required", [])

            for param_name, param_def in properties.items():
                config_params[param_name] = BrickParam(
                    type=param_def.get("type", "string"),
                    description=param_def.get("description", ""),
                    default=param_def.get("default"),
                    required=param_name in required_fields,
                    enum=param_def.get("enum"),
                )

            # Always add server and tool as implicit config
            brick = BrickSchema(
                name=brick_name,
                type="mcp",
                description=tool.get("description", f"MCP tool: {tool_name}"),
                when_to_use=f"Use when you need to call {server_name}:{tool_name}",
                category=f"mcp:{server_name}",
                config_schema={
                    "server": BrickParam(type="string", description="MCP server name", default=server_name),
                    "tool": BrickParam(type="string", description="Tool name", default=tool_name),
                    **config_params,
                },
            )

            self._bricks[brick_name] = brick
            count += 1

        return count

    def discover_all_mcp_servers(self, cache: SchemaCache = None) -> int:
        """Auto-discover bricks from all cached MCP servers.

        Returns:
            Total number of bricks discovered across all servers.
        """
        _cache = cache or SchemaCache()
        total = 0
        for server_name in _cache.list_cached_servers():
            total += self.discover_mcp_bricks(server_name, _cache)
        return total

    @property
    def count(self) -> int:
        """Total number of registered bricks."""
        return len(self._bricks)

    @property
    def builtin_count(self) -> int:
        """Number of built-in bricks."""
        return len(ALL_BUILTINS)
