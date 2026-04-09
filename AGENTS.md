# Repository Guidelines

## Project Structure & Module Organization
`src/brix/` contains the runtime, CLI, MCP server, runners, and DB-first persistence layer. Key subpackages are `bricks/`, `runners/`, and `mcp_handlers/`. Tests live in `tests/` and generally follow `test_<feature>.py`. Supporting materials live in `docs/`, reusable helper scripts in `helpers/`, pipeline examples and backups in `pipelines/`, and sample runtime data in `data/`.

## Build, Test, and Development Commands
Install locally with `python -m pip install -e ".[dev]"` from the repo root. Common commands:

- `python -m pytest` runs the full test suite.
- `python -m pytest tests/test_db.py -k migration` runs a focused subset.
- `ruff check .` enforces linting and import hygiene.
- `mypy src` runs strict type checking on the package.
- `docker compose up -d` starts the local containerized environment used in the README.
- `brix --help` or `python -m brix` verifies the CLI entrypoint.

## Coding Style & Naming Conventions
Target Python 3.12. Follow Ruff settings in `pyproject.toml`: 120-character lines, typed code, and clean imports. Mypy runs in `strict` mode, so prefer explicit types over inference at module boundaries. Use `snake_case` for functions, modules, and test files; `PascalCase` for classes; and keep new runner or handler names aligned with their existing namespace, for example `src/brix/mcp_handlers/runs.py`.

## Testing Guidelines
Use `pytest` with `pytest-asyncio`; async tests should be marked with `@pytest.mark.asyncio` when needed. Put regression tests next to the affected area and keep filenames descriptive, for example `tests/test_validator.py` or `tests/test_db_exec_brick.py`. Add focused tests for bug fixes and new behavior before widening scope. Prefer deterministic fixtures over live external services.

## Commit & Pull Request Guidelines
Recent history follows concise Conventional Commit prefixes such as `fix:` and `feat:`. Keep commit subjects short, imperative, and specific, for example `fix: preserve list params for db.exec`. PRs should state the behavioral change, note schema or migration impact, list validation steps run (`pytest`, `ruff`, `mypy`), and include screenshots only for UI-facing changes.

## Architecture & Contributor Notes
This repository is DB-first: SQLite-backed state such as pipelines, helpers, and metadata is central to the system. Avoid ad hoc file-format changes without checking how they interact with `src/brix/db.py`, migrations, and MCP CRUD handlers. When changing persistence or execution flow, update both runtime code and regression coverage.
