"""YAML pipeline loader with Jinja2 template support."""

import ast
import copy
import json
import os

import yaml
from jinja2 import ChainableUndefined
from jinja2.sandbox import SandboxedEnvironment

from brix.models import Pipeline, Step


class PipelineLoader:
    """Loads pipeline YAML files and renders Jinja2 templates."""

    def __init__(self) -> None:
        # SandboxedEnvironment prevents arbitrary code execution (D-13).
        # ChainableUndefined allows attribute chaining on undefined variables
        # (e.g. {{ skipped_step.output | default([]) }}) without raising
        # UndefinedError — callers use | default() for explicit fallbacks (D-16).
        self.env = SandboxedEnvironment(undefined=ChainableUndefined)
        # Add tojson filter so dicts/lists render as proper JSON, not Python repr
        self.env.filters["tojson"] = json.dumps
        # iif: inline if — {{ condition | iif('true_val', 'false_val') }}
        self.env.filters["iif"] = lambda val, true_val, false_val="": true_val if val else false_val
        # unwrap_foreach: extract data values from a ForeachResult items list
        self.env.filters["unwrap_foreach"] = lambda val: (
            [item.get("data") for item in val if item.get("success")]
            if isinstance(val, list)
            else val
        )

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def load(self, path: str, search_paths: list[str] | None = None) -> Pipeline:
        """Load a pipeline YAML file and parse into a Pipeline model."""
        with open(path) as f:
            raw = yaml.safe_load(f)
        base_dir = os.path.dirname(os.path.abspath(path))
        raw = self.resolve_includes(raw, base_dir=base_dir)
        raw = self.resolve_extends(raw, base_dir=base_dir, search_paths=search_paths)
        return Pipeline.model_validate(raw)

    def load_from_string(self, yaml_string: str, base_dir: str | None = None, search_paths: list[str] | None = None) -> Pipeline:
        """Load a pipeline from a YAML string."""
        raw = yaml.safe_load(yaml_string)
        raw = self.resolve_includes(raw, base_dir=base_dir)
        raw = self.resolve_extends(raw, base_dir=base_dir, search_paths=search_paths)
        return Pipeline.model_validate(raw)

    # ------------------------------------------------------------------
    # include: resolution (T-BRIX-V4-17)
    # ------------------------------------------------------------------

    def resolve_includes(self, raw: dict, base_dir: str | None = None) -> dict:
        """Expand ``include:`` references in the steps list.

        Each step may carry an ``include: <group_name>`` key instead of (or in
        addition to) the normal step fields.  When encountered, the step is
        replaced in-place by the steps of the named group (with a deep copy so
        repeated inclusions are independent).

        Groups are looked up in the following order:

        1. The ``groups:`` section of the current pipeline YAML.
        2. An external YAML file referenced as ``<file.yaml>`` (resolved
           relative to *base_dir* when given).  The external file must contain
           either a ``groups:`` mapping or a ``steps:`` list.  An optional
           fragment ``<file.yaml>#<group_name>`` selects a specific group from
           a multi-group file.

        After expansion the ``groups:`` key is left intact (it is accepted by
        the ``Pipeline`` model) so that the information is still available for
        inspection.

        Raises ``ValueError`` for unknown group references or circular
        inclusions.
        """
        if not isinstance(raw, dict):
            return raw

        groups: dict[str, list[dict]] = dict(raw.get("groups") or {})
        steps: list = raw.get("steps") or []

        # Track visited group names within this expansion to detect cycles.
        expanded = self._expand_steps(steps, groups, base_dir=base_dir, visited=set())
        result = dict(raw)
        result["steps"] = expanded
        return result

    def _expand_steps(
        self,
        steps: list,
        groups: dict[str, list[dict]],
        base_dir: str | None,
        visited: set,
    ) -> list:
        """Return a new step list with all ``include:`` entries expanded."""
        expanded: list[dict] = []
        for step in steps:
            if not isinstance(step, dict):
                expanded.append(step)
                continue
            include_ref = step.get("include")
            if include_ref is None:
                expanded.append(step)
                continue

            # Resolve include reference → list[dict]
            group_steps = self._resolve_group_ref(
                include_ref, groups, base_dir=base_dir, visited=visited
            )
            expanded.extend(group_steps)
        return expanded

    def _resolve_group_ref(
        self,
        ref: str,
        groups: dict[str, list[dict]],
        base_dir: str | None,
        visited: set,
    ) -> list[dict]:
        """Resolve a group reference string to a list of step dicts.

        Supports:
        - ``"group_name"`` — inline group defined in the pipeline's ``groups:`` section.
        - ``"file.yaml"`` — external file; must have a ``steps:`` key at the top level.
        - ``"file.yaml#group_name"`` — named group inside an external file.
        """
        if ref in visited:
            raise ValueError(
                f"Circular include detected: '{ref}' is already being expanded"
            )

        # --- 1. Inline group (no file extension marker) ---
        # If the ref doesn't look like a file path, try inline groups first.
        if "#" not in ref and not ref.endswith(".yaml") and not ref.endswith(".yml"):
            if ref in groups:
                visited_inner = visited | {ref}
                raw_steps = copy.deepcopy(groups[ref])
                return self._expand_steps(raw_steps, groups, base_dir=base_dir, visited=visited_inner)
            # Fall through to file resolution (maybe a short filename without extension)

        # --- 2. Inline group by exact name (handles names with dots) ---
        if ref in groups:
            visited_inner = visited | {ref}
            raw_steps = copy.deepcopy(groups[ref])
            return self._expand_steps(raw_steps, groups, base_dir=base_dir, visited=visited_inner)

        # --- 3. External file reference ---
        file_part, _, fragment = ref.partition("#")
        file_part = file_part.strip()
        fragment = fragment.strip()

        if not file_part:
            raise ValueError(f"Invalid include reference: '{ref}'")

        # Resolve file path
        if base_dir and not os.path.isabs(file_part):
            file_path = os.path.join(base_dir, file_part)
        else:
            file_path = file_part

        if not os.path.exists(file_path):
            raise ValueError(
                f"Include file not found: '{file_path}' (referenced as '{ref}')"
            )

        with open(file_path) as fh:
            ext_raw = yaml.safe_load(fh)

        if not isinstance(ext_raw, dict):
            raise ValueError(
                f"Include file '{file_path}' must be a YAML mapping"
            )

        ext_base_dir = os.path.dirname(os.path.abspath(file_path))

        if fragment:
            # Named group inside the external file
            ext_groups = ext_raw.get("groups") or {}
            if fragment not in ext_groups:
                raise ValueError(
                    f"Group '{fragment}' not found in '{file_path}'"
                )
            visited_inner = visited | {ref}
            raw_steps = copy.deepcopy(ext_groups[fragment])
            # Recursively expand includes within the external file, using its groups
            return self._expand_steps(
                raw_steps, ext_groups, base_dir=ext_base_dir, visited=visited_inner
            )
        else:
            # Whole-file inclusion — file must have a ``steps:`` key or ``groups:`` key
            if "steps" in ext_raw:
                visited_inner = visited | {ref}
                raw_steps = copy.deepcopy(ext_raw["steps"])
                ext_groups = ext_raw.get("groups") or {}
                return self._expand_steps(
                    raw_steps, ext_groups, base_dir=ext_base_dir, visited=visited_inner
                )
            raise ValueError(
                f"Include file '{file_path}' has no 'steps' key and no fragment was specified"
            )

    # ------------------------------------------------------------------
    # extends: resolution (T-BRIX-V6-18) — Pipeline-Template inheritance
    # ------------------------------------------------------------------

    def resolve_extends(
        self,
        raw: dict,
        base_dir: str | None = None,
        search_paths: list[str] | None = None,
    ) -> dict:
        """Resolve ``extends:`` inheritance for pipeline templates.

        When a pipeline specifies ``extends: <template-name>``, this method:

        1. Locates the base template YAML file.  The template name is resolved
           by searching (in order):
           a. *base_dir* — same directory as the instance pipeline file.
           b. Each path in *search_paths*.
           The name is tried as-is (e.g. ``extract-base.yaml``) and also with
           a ``.yaml`` suffix appended when the name has no extension.

        2. Loads the base template raw dict (runs ``resolve_includes`` on it
           too, so groups work inside templates).

        3. Substitutes ``{{ template.X }}`` placeholders in **every string
           value** of the base template's steps using the instance's
           ``template_params``.

        4. Merges the result: the instance's own fields (except ``extends`` and
           ``template_params``) override base fields; steps come from the
           rendered base (unless the instance also defines steps, in which case
           the instance steps win).

        If ``extends`` is not present the raw dict is returned unchanged.

        Raises ``ValueError`` for unknown template names or circular extends.
        """
        if not isinstance(raw, dict):
            return raw
        extends_name = raw.get("extends")
        if not extends_name:
            return raw

        # Locate the base template file
        template_raw = self._load_template_raw(
            extends_name, base_dir=base_dir, search_paths=search_paths
        )

        # Validate it is actually a template (kind: template) — soft check,
        # warn but don't block in case kind is omitted
        # (We just proceed; strict enforcement can be added later.)

        # Render {{ template.X }} substitutions in the template's raw dict
        template_params = raw.get("template_params") or {}
        rendered_base = self._render_template_params(template_raw, template_params)

        # Merge: start from rendered base, overlay instance fields
        # Instance fields take precedence; 'steps' from base are used unless
        # the instance defines its own non-empty steps.
        merged = dict(rendered_base)
        for key, value in raw.items():
            if key in ("extends", "template_params", "kind"):
                # Don't propagate inheritance meta-fields into merged result
                continue
            if key == "steps" and not value:
                # Empty or missing steps → inherit from base template
                continue
            merged[key] = value

        # Remove template-only fields that must not appear in the final pipeline
        merged.pop("kind", None)
        merged.pop("extends", None)
        merged.pop("template_params", None)

        return merged

    def _find_template_file(
        self,
        name: str,
        base_dir: str | None,
        search_paths: list[str] | None,
    ) -> str | None:
        """Return the absolute path to a template file, or None if not found."""
        candidates = [name]
        if not name.endswith(".yaml") and not name.endswith(".yml"):
            candidates.append(name + ".yaml")
            candidates.append(name + ".yml")

        search_dirs: list[str] = []
        if base_dir:
            search_dirs.append(base_dir)
        if search_paths:
            search_dirs.extend(search_paths)

        for directory in search_dirs:
            for candidate in candidates:
                full_path = os.path.join(directory, candidate)
                if os.path.isfile(full_path):
                    return os.path.abspath(full_path)

        return None

    def _load_template_raw(
        self,
        name: str,
        base_dir: str | None,
        search_paths: list[str] | None,
    ) -> dict:
        """Load a template YAML file and return the raw dict (with includes resolved)."""
        path = self._find_template_file(name, base_dir=base_dir, search_paths=search_paths)
        if path is None:
            searched = []
            if base_dir:
                searched.append(base_dir)
            if search_paths:
                searched.extend(search_paths)
            raise ValueError(
                f"Template '{name}' not found"
                + (f" (searched: {', '.join(searched)})" if searched else "")
            )

        with open(path) as f:
            raw = yaml.safe_load(f)

        if not isinstance(raw, dict):
            raise ValueError(f"Template file '{path}' must be a YAML mapping")

        template_base_dir = os.path.dirname(path)
        raw = self.resolve_includes(raw, base_dir=template_base_dir)
        return raw

    def _render_template_params(self, raw: dict, template_params: dict) -> dict:
        """Render ``{{ template.X }}`` placeholders throughout *raw* using *template_params*.

        The substitution context exposes params under the ``template`` key so that
        ``{{ template.script }}`` resolves to ``template_params['script']``.
        Only string values containing ``{{`` are rendered; everything else passes
        through unchanged (including non-string values).
        """
        context = {"template": template_params}
        return self._render_raw_dict(raw, context)

    def _render_raw_dict(self, value, context: dict):
        """Recursively render ``{{ template.X }}`` in a raw YAML structure."""
        if isinstance(value, str) and "{{" in value:
            rendered = self.render_template(value, context)
            return rendered
        elif isinstance(value, dict):
            return {k: self._render_raw_dict(v, context) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._render_raw_dict(item, context) for item in value]
        return value

    # ------------------------------------------------------------------
    # Jinja2 rendering
    # ------------------------------------------------------------------

    def render_template(self, template_string: str, context: dict) -> str:
        """Render a Jinja2 template string with the given context.

        User input is NEVER passed through Jinja2 — only pipeline-internal
        references (D-13).  Context typically contains: ``input.*``,
        ``credentials.*``, ``<step_id>.output``, and ``item``.
        """
        template = self.env.from_string(template_string)
        return template.render(context)

    def render_value(self, value, context: dict):
        """Render a value recursively.

        - ``str`` containing ``{{`` → rendered as Jinja2 template; if the
          result is valid JSON it is parsed and returned as the native type.
        - ``dict`` / ``list`` → recursed element by element.
        - Everything else → returned unchanged.
        """
        if isinstance(value, str) and "{{" in value:
            rendered = self.render_template(value, context)
            try:
                return json.loads(rendered)
            except (json.JSONDecodeError, ValueError):
                pass
            # Fallback: Python repr (Jinja2 renders dicts as {'key': 'val'})
            try:
                return ast.literal_eval(rendered)
            except (ValueError, SyntaxError):
                pass
            return rendered
        elif isinstance(value, dict):
            return {k: self.render_value(v, context) for k, v in value.items()}
        elif isinstance(value, list):
            return [self.render_value(item, context) for item in value]
        return value

    def render_step_params(self, step: Step, context: dict) -> dict:
        """Render all Jinja2 templates in a step's parameters.

        Returns a dict with the rendered ``params`` merged with rendered
        type-specific fields stored under ``_url``, ``_command``, ``_args``,
        and ``_headers`` keys.
        """
        rendered: dict = {}

        if step.params:
            rendered = self.render_value(step.params, context)

        # Render type-specific fields into reserved underscore keys so that
        # callers can retrieve them without risk of collision with user-defined
        # param names.
        if step.url:
            rendered["_url"] = self.render_value(step.url, context)
        if step.command:
            rendered["_command"] = self.render_value(step.command, context)
        if step.args:
            rendered["_args"] = self.render_value(step.args, context)
        if step.headers:
            rendered["_headers"] = self.render_value(step.headers, context)
        if getattr(step, "body", None) is not None:
            rendered["_body"] = self.render_value(step.body, context)
        if getattr(step, "pipeline", None):
            rendered["_pipeline"] = self.render_value(step.pipeline, context)

        # set runner: render 'values' dict and store under reserved key
        if getattr(step, "values", None):
            rendered["_values"] = self.render_value(step.values, context)

        return rendered

    # ------------------------------------------------------------------
    # Condition / foreach helpers
    # ------------------------------------------------------------------

    def evaluate_condition(self, condition: str | None, context: dict) -> bool:
        """Evaluate a ``when`` condition.

        Returns ``True`` if the step should execute.  An empty or ``None``
        condition always returns ``True``.  The rendered string is compared
        against a set of canonical falsy representations.
        """
        if not condition:
            return True
        rendered = self.render_template(condition, context)
        return rendered.lower() not in ("false", "0", "", "none", "[]", "{}")

    def resolve_foreach(self, foreach_expr: str, context: dict) -> list:
        """Resolve a ``foreach`` expression to a Python list.

        Accepts:
        - A template that renders directly to a ``list``.
        - A template whose rendered result is a JSON-encoded list string.

        Raises ``ValueError`` for anything else.
        """
        result = self.render_value(foreach_expr, context)

        if isinstance(result, list):
            return result

        if isinstance(result, str):
            # Try JSON first (canonical representation)
            try:
                parsed = json.loads(result)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass

            # Fallback: Python literal repr (e.g. "['a', 'b']" from Jinja2 str coercion)
            try:
                parsed = ast.literal_eval(result)
                if isinstance(parsed, list):
                    return parsed
            except (ValueError, SyntaxError):
                pass

        raise ValueError(
            f"foreach expression did not resolve to a list: {foreach_expr!r} → {result!r}"
        )
