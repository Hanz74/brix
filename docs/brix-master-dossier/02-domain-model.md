# Domain Model

## Purpose
This document defines the core entities, relationships, mandatory fields, and lifecycle states required for a complete Brix platform model.

## Entity Catalog

### Intent
Represents the original user need, request, or operational problem.

Required fields:
- `intent_id`
- `project`
- `raw_text`
- `normalized_summary`
- `origin`
- `created_at`
- `status`

Recommended fields:
- `domain`
- `constraints`
- `expected_outcome`
- `risk_level`
- `related_intents`

### Task
Represents an implementation or repair unit derived from an intent.

Required fields:
- `task_id`
- `project`
- `title`
- `description`
- `status`
- `owner`
- `source_intent_id`
- `created_at`

Recommended fields:
- `epic_id`
- `wave_id`
- `acceptance_criteria`
- `risk_notes`

### Decision
Represents an architectural, operational, or policy decision.

Required fields:
- `decision_id`
- `project`
- `title`
- `statement`
- `rationale`
- `status`
- `created_at`

Recommended fields:
- `replaced_by`
- `related_components`

### Pipeline
Represents a DB-backed orchestration artifact.

Required fields:
- `pipeline_id`
- `project`
- `name`
- `description`
- `version`
- `owner`
- `purpose`
- `status`

Recommended fields:
- `intent_id`
- `primary_domain`
- `deprecation_note`
- `replacement_pipeline_id`

### Step
Represents a pipeline step in its persisted raw form and in its effective materialized form.

Required fields:
- `step_id`
- `pipeline_id`
- `effective_type`
- `enabled`
- `status`

Recommended fields:
- `raw_shape`
- `materialized_shape`
- `policy_flags`
- `workaround_flags`

### Brick
Represents a reusable capability with schema and lifecycle.

Required fields:
- `brick_id`
- `name`
- `namespace`
- `description`
- `runner`
- `input_contract`
- `output_contract`
- `owner`
- `status`

Recommended fields:
- `aliases`
- `anti_patterns`
- `replacement_brick`
- `usage_examples`

### Helper
Represents a script-backed implementation artifact. In the target model it is transitional or exceptional.

Required fields:
- `helper_id`
- `name`
- `description`
- `owner`
- `project`
- `status`
- `script_path`

Recommended fields:
- `brick_candidate`
- `reason_not_a_brick`
- `replacement_brick`

### Connection
Represents a named DB or service connection.

Required fields:
- `connection_id`
- `name`
- `driver`
- `project`
- `owner`
- `status`

### Variable
Represents a managed value used in config or runtime.

Required fields:
- `variable_id`
- `name`
- `project`
- `description`
- `owner`
- `classification`

### Run
Represents execution history.

Required fields:
- `run_id`
- `pipeline_id`
- `status`
- `started_at`
- `finished_at`

Recommended fields:
- `phase`
- `failed_step_id`
- `root_cause`
- `source_intent_id`

### Finding
Represents validator, drift, governance, or runtime findings.

Required fields:
- `finding_id`
- `code`
- `severity`
- `category`
- `message`
- `target_type`
- `target_id`
- `created_at`

### HelpTopic
Represents system help content. Must be tied to current product truth.

Required fields:
- `help_topic_id`
- `name`
- `title`
- `content`
- `status`
- `owner`

### ChangelogEntry
Represents persistent release and change history.

Required fields:
- `entry_id`
- `version`
- `type`
- `summary`
- `created_at`

### WorkaroundPattern
Represents a known anti-pattern or workaround form.

Required fields:
- `pattern_id`
- `code`
- `name`
- `description`
- `risk`
- `recommended_replacement`

### ReuseCandidate
Represents an inferred candidate for a new reusable brick or compositional artifact.

Required fields:
- `candidate_id`
- `pattern_summary`
- `evidence`
- `priority`
- `status`

## Core Relationships

- `Intent -> led_to -> Task`
- `Task -> changed -> Pipeline`
- `Task -> created -> Brick`
- `Decision -> governs -> Component`
- `Pipeline -> contains -> Step`
- `Step -> uses -> Brick`
- `Step -> references -> Helper`
- `Pipeline -> invokes -> Pipeline`
- `Pipeline -> depends_on -> Connection`
- `Pipeline -> uses -> Variable`
- `Run -> executed -> Pipeline`
- `Run -> failed_at -> Step`
- `Finding -> points_to -> Component`
- `HelpTopic -> documents -> Brick`
- `ChangelogEntry -> records -> Task`
- `WorkaroundPattern -> detected_in -> Step`
- `ReuseCandidate -> derived_from -> StepPattern`
- `Intent -> resembles -> Intent`

## Lifecycle States

### Universal States
- `draft`
- `active`
- `deprecated`
- `replaced`
- `archived`

### Additional Execution States
- `pending`
- `running`
- `success`
- `failure`
- `aborted`
- `incomplete`

## Invariants

### Invariant 1
No production-relevant component may be `active` without required metadata.

### Invariant 2
No step may be executed without a materialized effective shape.

### Invariant 3
No new helper may be introduced without a brick-candidate evaluation.

### Invariant 4
No new pipeline may be accepted without a reuse check.

### Invariant 5
No component help topic may describe legacy or superseded behavior without an explicit deprecation context.

### Invariant 6
No detected workaround pattern may remain invisible to validator, diagnosis, or governance surfaces.
