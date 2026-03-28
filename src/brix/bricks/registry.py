"""Brick registry: built-in bricks + MCP auto-discovery."""
import json
import logging
from typing import Optional

from brix.bricks.schema import BrickParam, BrickSchema
from brix.bricks.builtins import ALL_BUILTINS
from brix.cache import SchemaCache

logger = logging.getLogger(__name__)


def _row_to_brick(row: dict) -> BrickSchema:
    """Convert a brick_definitions DB row to a BrickSchema instance."""
    # Deserialise config_schema: dict[str, plain dict] → dict[str, BrickParam]
    raw_schema = row.get("config_schema", "{}")
    if isinstance(raw_schema, str):
        raw_schema = json.loads(raw_schema)
    config_schema: dict[str, BrickParam] = {}
    for param_name, param_def in raw_schema.items():
        config_schema[param_name] = BrickParam(
            type=param_def.get("type", "string"),
            description=param_def.get("description", ""),
            default=param_def.get("default"),
            required=bool(param_def.get("required", False)),
            enum=param_def.get("enum"),
        )

    aliases = row.get("aliases", "[]")
    if isinstance(aliases, str):
        aliases = json.loads(aliases)

    examples = row.get("examples", "[]")
    if isinstance(examples, str):
        examples = json.loads(examples)

    return BrickSchema(
        name=row["name"],
        type=row.get("runner", ""),
        runner=row.get("runner", ""),
        namespace=row.get("namespace", ""),
        category=row.get("category", "general"),
        description=row.get("description", ""),
        when_to_use=row.get("when_to_use", ""),
        when_NOT_to_use=row.get("when_NOT_to_use", ""),
        aliases=aliases,
        input_type=row.get("input_type", ""),
        output_type=row.get("output_type", ""),
        config_schema=config_schema,
        examples=examples,
        related_connector=row.get("related_connector", ""),
        system=bool(row.get("system", False)),
    )


class BrickRegistry:
    """Central registry of all available bricks."""

    def __init__(self, db=None):
        self._bricks: dict[str, BrickSchema] = {}
        self._db = db
        self._load_builtins()

    def _load_builtins(self):
        """Register all built-in bricks — from DB if available, else from code."""
        loaded_from_db = False
        if self._db is not None:
            try:
                rows = self._db.brick_definitions_list()
                if rows:
                    for row in rows:
                        try:
                            brick = _row_to_brick(row)
                            self._bricks[brick.name] = brick
                        except Exception as e:
                            logger.warning("Could not load brick '%s' from DB: %s", row.get("name"), e)
                    loaded_from_db = True
            except Exception as e:
                logger.warning("Could not load bricks from DB: %s", e)

        if not loaded_from_db:
            for brick in ALL_BUILTINS:
                self._bricks[brick.name] = brick

    def register(self, brick: BrickSchema):
        """Register a custom brick."""
        self._bricks[brick.name] = brick

    def unregister(self, name: str):
        """Remove a brick from registry.

        Raises ValueError if the brick is a system brick (system=True).
        """
        brick = self._bricks.get(name)
        if brick is not None and brick.system:
            raise ValueError(f"Cannot unregister system brick '{name}'")
        self._bricks.pop(name, None)

    def get(self, name: str, _seen: set | None = None) -> Optional[BrickSchema]:
        """Get a brick by name, resolving inheritance (T-BRIX-DB-23).

        If the brick declares ``extends``, the parent brick is loaded and the
        child's fields are merged on top of the parent (child overrides parent).
        Cycle detection prevents infinite loops.
        """
        brick = self._bricks.get(name)
        if brick is None:
            return None
        if not brick.extends:
            return brick

        # Cycle detection
        if _seen is None:
            _seen = set()
        if name in _seen:
            logger.warning("Brick inheritance cycle detected at '%s' — returning brick as-is", name)
            return brick
        _seen.add(name)

        parent = self.get(brick.extends, _seen=_seen)
        if parent is None:
            logger.warning("Brick '%s' extends unknown parent '%s' — returning child as-is", name, brick.extends)
            return brick

        # Merge: start from parent dict, overlay child non-default values
        parent_dict = parent.model_dump()
        child_dict = brick.model_dump()

        # Fields set explicitly on the child override the parent.
        # We identify "explicitly set" as any field where the child differs
        # from its own default value (i.e. not the BrickSchema class default).
        merged = parent_dict.copy()
        # Get field defaults from the class model_fields (not an instance)
        _field_defaults: dict = {}
        for field_name, field_info in BrickSchema.model_fields.items():
            _field_defaults[field_name] = field_info.default
        for field_name, child_val in child_dict.items():
            default_val = _field_defaults.get(field_name)
            if child_val != default_val:
                merged[field_name] = child_val
            # extends itself should not be propagated to the resolved brick
        merged["extends"] = None  # Resolved — remove inheritance marker
        merged["name"] = name  # Keep child's name

        try:
            return BrickSchema.model_validate(merged)
        except Exception as e:
            logger.warning("Brick inheritance merge failed for '%s': %s", name, e)
            return brick

    def list_all(self) -> list[BrickSchema]:
        """List all registered bricks."""
        return list(self._bricks.values())

    def list_by_category(self, category: str) -> list[BrickSchema]:
        """List bricks filtered by category."""
        return [b for b in self._bricks.values() if b.category == category]

    def search(self, query: str, category: str = None) -> list[BrickSchema]:
        """Search bricks by keyword in name, description, when_to_use, and aliases.

        Optional category filter. Case-insensitive.
        """
        query_lower = query.lower()
        results = []
        for brick in self._bricks.values():
            if category and brick.category != category:
                continue
            alias_text = " ".join(brick.aliases).lower()
            searchable = (
                f"{brick.name} {brick.description} {brick.when_to_use} "
                f"{brick.when_NOT_to_use} {alias_text}"
            ).lower()
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
        """Number of built-in bricks (from DB if loaded, else from code)."""
        if self._db is not None:
            try:
                count = self._db.brick_definitions_count()
                if count > 0:
                    return count
            except Exception:
                pass
        return len(ALL_BUILTINS)
