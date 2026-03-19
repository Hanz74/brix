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
# ENTRYPOINT uses brix CLI
ENTRYPOINT ["brix"]
