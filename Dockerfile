FROM python:3.12-slim

LABEL maintainer="Brix Project"
LABEL description="Brix — Generischer Prozess-Orchestrator fuer Claude Code Skills"

# Install uv
RUN pip install uv

WORKDIR /app

# Copy project metadata and source
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Install project with dev dependencies via uv
RUN uv pip install --system -e ".[dev]"

# Docker socket is mounted via volume at runtime
# Supports two modes:
#   BRIX_MODE=mcp   → start as MCP server (stdio transport)
#   default         → container stays alive; brix is invoked via docker exec
CMD ["sh", "-c", "if [ \"$BRIX_MODE\" = 'mcp' ]; then exec brix mcp; else exec sleep infinity; fi"]
