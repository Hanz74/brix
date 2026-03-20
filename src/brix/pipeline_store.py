"""Pipeline persistence: save, load, list, version."""
import yaml
from pathlib import Path
from typing import Optional

from brix.loader import PipelineLoader
from brix.models import Pipeline

DEFAULT_PIPELINES_DIR = Path.home() / ".brix" / "pipelines"


class PipelineStore:
    """Manages pipeline YAML files."""

    def __init__(self, pipelines_dir: Optional[Path] = None):
        self.pipelines_dir = pipelines_dir or DEFAULT_PIPELINES_DIR
        self.pipelines_dir.mkdir(parents=True, exist_ok=True)
        self.loader = PipelineLoader()

    def save(self, pipeline_data: dict, name: Optional[str] = None) -> Path:
        """Save pipeline data as YAML. Returns the file path."""
        pipeline_name = name or pipeline_data.get("name", "unnamed")
        filename = f"{pipeline_name}.yaml"
        path = self.pipelines_dir / filename

        with open(path, "w") as f:
            yaml.dump(pipeline_data, f, default_flow_style=False, allow_unicode=True)

        return path

    def load(self, name: str) -> Pipeline:
        """Load a pipeline by name. Tries name.yaml and name.yml."""
        for ext in [".yaml", ".yml"]:
            path = self.pipelines_dir / f"{name}{ext}"
            if path.exists():
                return self.loader.load(str(path))
        raise FileNotFoundError(f"Pipeline '{name}' not found in {self.pipelines_dir}")

    def load_raw(self, name: str) -> dict:
        """Load pipeline as raw dict (for inspection/modification)."""
        for ext in [".yaml", ".yml"]:
            path = self.pipelines_dir / f"{name}{ext}"
            if path.exists():
                with open(path) as f:
                    return yaml.safe_load(f) or {}
        raise FileNotFoundError(f"Pipeline '{name}' not found")

    def exists(self, name: str) -> bool:
        """Check if a pipeline exists."""
        return any(
            (self.pipelines_dir / f"{name}{ext}").exists()
            for ext in [".yaml", ".yml"]
        )

    def list_all(self) -> list[dict]:
        """List all saved pipelines with metadata."""
        results = []
        files = sorted(self.pipelines_dir.glob("*.yaml")) + sorted(
            self.pipelines_dir.glob("*.yml")
        )
        for f in files:
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
        """Delete a pipeline. Returns True if deleted."""
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
