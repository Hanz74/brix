# Changelog

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
