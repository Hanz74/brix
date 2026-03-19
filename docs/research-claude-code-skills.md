# Claude Code Custom Slash Commands Research

**Date:** 2026-03-19

---

## 1. File Locations

| Location | Scope | Label in `/help` |
|----------|-------|------------------|
| `~/.claude/commands/*.md` | Global (all projects) | `(user)` |
| `.claude/commands/*.md` | Project-specific | `(project)` |
| `.claude/skills/<name>/SKILL.md` | Skill format (newer) | same |

Subdirectories create namespaces: `.claude/commands/git/commit.md` → `/git:commit`

## 2. Format: Frontmatter + Markdown

Plain Markdown with optional YAML frontmatter:

```yaml
---
description: Short description for /help (~60 chars)
argument-hint: <search-query> [--output DIR]
allowed-tools: Bash(brix:*), Read, Grep
model: sonnet
---

Your prompt content here with $ARGUMENTS
```

**Key frontmatter fields:**
- `description` — shown in autocomplete and `/help`
- `argument-hint` — shown after command name in autocomplete
- `allowed-tools` — pre-approved tools (no permission prompt)
- `model` — override model for this skill
- `disable-model-invocation` — only manually invocable

## 3. Arguments

```
User types: /download-attachments invoices from march
```

Available in prompt:
- `$ARGUMENTS` → `invoices from march` (full string)
- `$1` → `invoices`
- `$2` → `from`
- `$3` → `march`

## 4. Bash Execution — Two Mechanisms

### A) Inline execution with `!` backtick syntax

Executed BEFORE prompt is sent to Claude. Output is injected into the prompt:

```markdown
Current status: !`brix validate pipeline.yaml 2>&1`
```

### B) Claude executes Bash via tool

Standard mechanism — Claude calls the Bash tool during conversation:

```markdown
Run: `brix run download-attachments.yaml --query "$ARGUMENTS"`
```

**For Brix:** Option A is ideal for validation/status checks. Option B for the actual pipeline run where Claude needs to process the result.

## 5. Output Handling

Claude sees stdout + stderr fully. JSON on stdout is **not automatically parsed** — but Claude understands JSON natively and can interpret it without additional instruction.

Best practice:
```markdown
Execute: !`brix run pipeline.yaml --query "$ARGUMENTS" 2>&1`

Parse the JSON result and present:
- Summary table of processed items
- Error details for failures
- Total count and size
```

## 6. Nesting

Skills cannot directly invoke other skills (`/other-skill`). But Claude can spawn subagents via the Task tool that follow instructions from other skill files.

## 7. Error Handling

Claude sees stderr fully. Exit codes are visible. Best practice — handle in prompt:

```markdown
!`brix run pipeline.yaml 2>&1 || echo "BRIX_FAILED"`

If BRIX_FAILED appears: analyze error, report to user with cause and fix suggestion.
```

## 8. Limitations

| Limitation | Details |
|-----------|---------|
| Max prompt length | No documented limit for `.md` file; practical limit is model context window |
| Relative file refs | `@path/to/file.md` injects file content into prompt |
| Static includes | `@relative/path` works for injecting context files |

## 9. Recommended Brix Skill Structure

```yaml
---
description: Download M365 attachments via Brix pipeline
argument-hint: <search-query> [--output DIR]
allowed-tools: Bash(brix:*)
---
```

```markdown
Run the Brix download pipeline:

Validate: !`brix validate pipelines/download-attachments.yaml 2>&1`

Execute:
\`\`\`bash
brix run pipelines/download-attachments.yaml --query "$ARGUMENTS" --format json
\`\`\`

Parse the JSON result and present:
- Summary table with downloaded files
- Error details for failures
- Total count and size
```

**Key insight:** `allowed-tools: Bash(brix:*)` pre-approves all brix commands — no permission prompts for the user. This makes the skill feel seamless.

## 10. Existing Skill

A `/download-attachments` skill already exists at `~/.claude/commands/download-attachments.md` — this can serve as the first Brix migration target.
