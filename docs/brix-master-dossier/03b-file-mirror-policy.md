# File Mirror Policy

## Purpose
This policy removes ambiguity around file-backed state. Brix is DB-first: operational authoring truth lives in the database. Files are allowed only as non-authoritative artifacts.

## Authoritative Truth
The DB owns live component state:

- pipeline metadata and normalized steps
- helper registry metadata and stored helper code where available
- brick definitions, tool schemas, triggers, variables, profiles, changelog, and governance metadata
- run history, diagnostics, pins, and persisted step data

If DB state and file state disagree, the DB is correct for authoring, inspection, validation, execution, and repair.

## Allowed File Mirror Purposes
Files may exist for these purposes only:

- `export`: a human-readable snapshot or generated artifact
- `backup`: rollback support or disaster recovery material
- `bundle`: project/package transfer across Brix instances
- `debug`: temporary diagnostic output
- `legacy_import`: historical YAML or registry input being imported into DB state

None of these purposes makes a file authoritative.

## Forbidden Uses
Brix tools, agents, and migrations must not:

- repair live pipeline state by editing YAML mirrors directly
- treat `yaml_content` as the primary pipeline body
- prefer disk files over normalized DB rows
- hide missing DB rows because a mirror file exists
- create new file-first authoring flows

## Tooling Boundary
Tools may read files when importing, exporting, bundling, backing up, or debugging. Tools that create or modify live Brix entities must write DB rows first and may emit file artifacts only as explicit exports.

Integrity checks must report missing or inconsistent DB state as DB issues. A file mirror can provide migration input, but it must not suppress an authoring-truth problem.

## Agent Rule
When an LLM sees a YAML file, helper file, bundle, or exported artifact, it must ask whether the corresponding DB entity exists and use DB/MCP inspection as the source of truth before making changes.
