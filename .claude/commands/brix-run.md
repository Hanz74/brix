---
description: Run any Brix pipeline
argument-hint: <pipeline-name> [params...]
allowed-tools: Bash(brix:*)
---

# Brix Pipeline Runner

Run a Brix pipeline by name.

## Arguments

Parse `$ARGUMENTS` for:
- First argument: pipeline name (e.g. "download-attachments")
- Remaining arguments: pipeline parameters as key=value pairs

## Execution

1. Resolve the pipeline name from the first argument.

2. Validate first:
```bash
brix validate <pipeline-name>
```

3. Execute:
```bash
brix run <pipeline-name> <params as -p key=value>
```

## Result

Parse JSON output and present a clean summary to the user.
Show step-by-step status if there were errors.
