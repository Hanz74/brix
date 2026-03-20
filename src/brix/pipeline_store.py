"""Pipeline persistence: save, load, list, version."""
import yaml
from pathlib import Path
from typing import Optional

from brix.loader import PipelineLoader
from brix.models import Pipeline

DEFAULT_PIPELINES_DIR = Path.home() / ".brix" / "pipelines"

DEFAULT_SEARCH_PATHS = [
    Path.home() / ".brix" / "pipelines",  # User-Pipelines
    Path("/app/pipelines"),                 # Container Volume-Mount
]


class PipelineStore:
    """Manages pipeline YAML files.

    Searches multiple directories for pipelines (search_paths) but always
    saves to pipelines_dir (the primary/user directory).
    """

    def __init__(
        self,
        pipelines_dir: Optional[Path] = None,
        search_paths: Optional[list] = None,
    ):
        self.pipelines_dir = Path(pipelines_dir) if pipelines_dir is not None else DEFAULT_PIPELINES_DIR
        self.pipelines_dir.mkdir(parents=True, exist_ok=True)
        if search_paths is not None:
            # Caller provided explicit search_paths — use as-is
            self.search_paths = [Path(p) for p in search_paths]
        elif pipelines_dir is not None:
            # Custom pipelines_dir: prepend it to the default search paths
            extra = [Path(pipelines_dir)]
            self.search_paths = extra + [
                p for p in DEFAULT_SEARCH_PATHS if Path(p) != Path(pipelines_dir)
            ]
        else:
            self.search_paths = DEFAULT_SEARCH_PATHS
        self.loader = PipelineLoader()

    def save(self, pipeline_data: dict, name: Optional[str] = None) -> Path:
        """Save pipeline data as YAML to pipelines_dir. Returns the file path."""
        pipeline_name = name or pipeline_data.get("name", "unnamed")
        filename = f"{pipeline_name}.yaml"
        path = self.pipelines_dir / filename

        with open(path, "w") as f:
            yaml.dump(pipeline_data, f, default_flow_style=False, allow_unicode=True)

        return path

    def load(self, name: str) -> Pipeline:
        """Load a pipeline by name. Searches all search_paths in order."""
        for search_dir in self.search_paths:
            for ext in [".yaml", ".yml"]:
                path = Path(search_dir) / f"{name}{ext}"
                if path.exists():
                    return self.loader.load(str(path))
        raise FileNotFoundError(
            f"Pipeline '{name}' not found in: {[str(p) for p in self.search_paths]}"
        )

    def load_raw(self, name: str) -> dict:
        """Load pipeline as raw dict (for inspection/modification). Searches all search_paths."""
        for search_dir in self.search_paths:
            for ext in [".yaml", ".yml"]:
                path = Path(search_dir) / f"{name}{ext}"
                if path.exists():
                    with open(path) as f:
                        return yaml.safe_load(f) or {}
        raise FileNotFoundError(f"Pipeline '{name}' not found")

    def exists(self, name: str) -> bool:
        """Check if a pipeline exists in any search path."""
        for search_dir in self.search_paths:
            if any(
                (Path(search_dir) / f"{name}{ext}").exists()
                for ext in [".yaml", ".yml"]
            ):
                return True
        return False

    def list_all(self) -> list[dict]:
        """List pipelines from ALL search paths, deduplicated by name (first path wins)."""
        seen: set[str] = set()
        results = []
        for search_dir in self.search_paths:
            search_dir = Path(search_dir)
            if not search_dir.exists():
                continue
            files = sorted(search_dir.glob("*.yaml")) + sorted(search_dir.glob("*.yml"))
            for f in files:
                if f.stem in seen:
                    continue
                seen.add(f.stem)
                try:
                    pipeline = self.loader.load(str(f))
                    results.append(
                        {
                            "name": pipeline.name,
                            "version": pipeline.version,
                            "description": pipeline.description or "",
                            "steps": len(pipeline.steps),
                            "path": str(f),
                        }
                    )
                except Exception as e:
                    results.append(
                        {
                            "name": f.stem,
                            "version": "?",
                            "description": f"Error: {e}",
                            "steps": 0,
                            "path": str(f),
                        }
                    )
        return results

    def delete(self, name: str) -> bool:
        """Delete a pipeline from pipelines_dir. Returns True if deleted."""
        for ext in [".yaml", ".yml"]:
            path = self.pipelines_dir / f"{name}{ext}"
            if path.exists():
                path.unlink()
                return True
        return False

    def get_version(self, name: str) -> str:
        """Get the current version of a pipeline."""
        pipeline = self.load(name)
        return pipeline.version
