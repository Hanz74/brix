# Changelog

## [0.2.3] — 2026-03-19

### Added
- Retry logic with linear/exponential backoff (`on_error: retry`, `RetryConfig`)
- Default 60s timeout for steps without explicit timeout (via `parse_timeout`)

## [0.2.2] — 2026-03-19

### Added
- foreach + parallel execution with concurrency limits (D-15 partial-success)

## [0.2.0] — 2026-03-19

### Added
- Python runner: script execution via subprocess (D-19)

## [0.1.2] — 2026-03-19

### Added
- Click CLI with `brix run`, `brix validate`, `--dry-run`
- Parameter passing via `-p key=value`
- Pipeline execution with JSON output on stdout
- Dry-run mode showing step details

## [0.1.1] — 2026-03-19

### Changed
- Implemented Pydantic models for Pipeline, Step, StepResult, RunResult, ServerConfig

## [0.1.0] — 2026-03-19

### Added
- Initial project structure
- pyproject.toml with Hatchling build system
- Source layout under src/brix/
- Test infrastructure with pytest + pytest-asyncio
