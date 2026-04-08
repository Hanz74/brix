# Help Surface Audit

Source audited: `seed-data.json` `help_topics`

Audit focus:
- Legacy step type names: `http`, `python`, `mcp`, `filter`, `transform`, `set`
- YAML/file-based references in user-facing help
- Outdated MCP tool names
- Guidance that contradicts Brick-first / DB-first positioning

## anti-patterns
Issues found:
- Uses outdated tool name `brix__credential_add`.
- Mentions YAML, but only as a forbidden anti-pattern. This is not a behavioral contradiction.

Suggested fix:
- Replace `brix__credential_add` with `brix__credential(action="add", ...)`.
- Keep the anti-YAML warning, but phrase it as a legacy mirror/backup path rather than an editable surface.

## beispiele
Issues found:
- Uses legacy step types `http`, `python`, and `mcp`.
- Uses outdated pipeline input shape via `params=[...]` instead of current `input_schema`.
- Examples lean script-first instead of Brick-first.

Suggested fix:
- Rewrite examples to use canonical brick names such as `http.request`, `script.python`, and `mcp.call`.
- Replace `params` examples with `input_schema`.
- Prefer purpose-built bricks first, and only use `script.python` where custom logic is genuinely required.

## credentials
Issues found:
- Uses legacy step type `python`.
- Uses outdated tool names: `brix__credential_add`, `brix__credential_list`, `brix__credential_get`, `brix__credential_update`, `brix__credential_rotate`, `brix__credential_search`, `brix__credential_delete`.

Suggested fix:
- Replace the step example with `script.python`.
- Consolidate all tool references to `brix__credential(action="...", ...)`.

## dag
Issues found:
- Uses legacy step type `python`.

Suggested fix:
- Replace example step types with canonical brick names, using `script.python` only if no narrower brick fits the example.

## debugging
Issues found:
- None.

Suggested fix:
- No change required.

## error-patterns
Issues found:
- Uses outdated tool names `brix__registry_add` and `brix__registry_search`.

Suggested fix:
- Replace them with `brix__registry(action="add", ...)` and `brix__registry(action="search", ...)`.

## foreach
Issues found:
- Uses legacy step type `python`.

Suggested fix:
- Replace the loop example with canonical brick names, for example `script.python` if the example remains code-based.

## helpers
Issues found:
- Uses legacy step type `python`.
- Shows `brix__register_helper(name="my_processor")` as if registration were a separate no-arg follow-up to `create_helper`.
- Topic framing still treats helpers as the default extension path instead of a narrower escape hatch.

Suggested fix:
- Replace `python` with `script.python`.
- Update the workflow to say `brix__create_helper(...)` already registers the helper, and only document `brix__register_helper(...)` for externally authored scripts with an explicit `script` path.
- Reframe helpers as optional for logic that cannot be expressed cleanly with existing bricks.

## lessons-learned
Issues found:
- Uses outdated tool name `brix__server_health`.

Suggested fix:
- Replace it with `brix__server(action="health", ...)` or `brix__health`, depending on the intended scope.

## org-fields
Issues found:
- None.

Suggested fix:
- No change required.

## pipeline-persistence
Issues found:
- User-facing help still exposes `yaml backup` wording and the `BRIX_STEP_SOURCE` values `yaml-mode` / `dual`.
- This weakens the DB-only message even though the topic otherwise describes DB-first persistence correctly.

Suggested fix:
- Keep the statement that YAML is legacy/internal only.
- Remove or heavily de-emphasize `yaml-mode` and `dual` from end-user help, or move them into low-level debugging docs.

## quick-start
Issues found:
- Uses legacy step types `http` and `python`.
- Uses `brix__run_pipeline(name="hello-world")`, which drifts from the current `pipeline_id` parameter name.
- Mentions YAML only as a forbidden anti-pattern; that part is directionally correct.

Suggested fix:
- Replace step types with `http.request` and `script.python`.
- Update the run example to `brix__run_pipeline(pipeline_id="hello-world")`.

## registries
Issues found:
- Uses outdated tool names `brix__registry_add`, `brix__registry_get`, `brix__registry_list`, `brix__registry_update`, `brix__registry_delete`, `brix__registry_search`.
- Uses outdated tool name `brix__server_health`.
- Describes the `patterns` registry as reusable step patterns with `YAML`, which conflicts with DB-first / Brick-first guidance.

Suggested fix:
- Replace all registry CRUD references with `brix__registry(action="...", ...)`.
- Replace `brix__server_health` with `brix__server(action="health", ...)` or `brix__health`.
- Reword `patterns` as brick/pipeline composition patterns instead of YAML patterns.

## sdk
Issues found:
- None.

Suggested fix:
- No change required.

## step-referenzen
Issues found:
- None.

Suggested fix:
- No change required.

## templates
Issues found:
- Uses legacy step types `python` and `http`.
- Centers the topic on `extends`, while current user-facing template flow is represented by `brix__list_templates` and `brix__instantiate_template`.
- Uses `brix__get_template(name="...")`, but current help/tooling positions `get_template` as goal-based discovery rather than named template retrieval.

Suggested fix:
- Rewrite around blueprint templates: discover with `brix__list_templates`, instantiate with `brix__instantiate_template`, and only mention `extends` if it remains a supported low-level implementation detail.
- Replace step types with canonical brick names.
- Remove the `brix__get_template(name=...)` example or change it to the current discovery usage.

## tools
Issues found:
- Uses many outdated tool names for consolidated action-based APIs.
- Affected groups include:
- Alerts: `brix__alert_add`, `brix__alert_list`, `brix__alert_update`, `brix__alert_delete`, `brix__alert_history`
- Credentials: `brix__credential_add`, `brix__credential_list`, `brix__credential_get`, `brix__credential_update`, `brix__credential_rotate`, `brix__credential_search`, `brix__credential_delete`
- Servers: `brix__server_add`, `brix__server_list`, `brix__server_update`, `brix__server_remove`, `brix__server_refresh`, `brix__server_health`
- State: `brix__state_get`, `brix__state_set`, `brix__state_list`, `brix__state_delete`
- Triggers: `brix__trigger_add`, `brix__trigger_get`, `brix__trigger_list`, `brix__trigger_update`, `brix__trigger_delete`, `brix__trigger_test`
- Trigger groups: `brix__trigger_group_add`, `brix__trigger_group_list`, `brix__trigger_group_start`, `brix__trigger_group_stop`, `brix__trigger_group_delete`

Suggested fix:
- Rewrite the decision tree to use the consolidated tools:
- `brix__alert(action="...", ...)`
- `brix__credential(action="...", ...)`
- `brix__server(action="...", ...)`
- `brix__state(action="...", ...)`
- `brix__trigger(action="...", ...)`
- `brix__trigger_group(action="...", ...)`

## triggers
Issues found:
- Mentions `schedules.yaml` as migration context.
- Contains a legacy `mcp` step-type reference in example material.

Suggested fix:
- Keep the migration note brief, but center the topic entirely on DB-native triggers.
- Replace any `mcp` step example with `mcp.call`.
