"""Pipeline and brick discovery and registration."""
import yaml
from pathlib import Path
from typing import Optional

from brix.loader import PipelineLoader

PIPELINES_DIR = Path.home() / ".brix" / "pipelines"
BRICKS_DIR = Path.home() / ".brix" / "bricks"


class BrixRegistry:
    def __init__(self, pipelines_dir: Path = None, bricks_dir: Path = None):
        self.pipelines_dir = pipelines_dir or PIPELINES_DIR
        self.bricks_dir = bricks_dir or BRICKS_DIR
        self.loader = PipelineLoader()

    def list_pipelines(self) -> list[dict]:
        """List all available pipelines with metadata."""
        if not self.pipelines_dir.exists():
            return []

        results = []
        yaml_files = sorted(self.pipelines_dir.glob("*.yaml")) + sorted(
            self.pipelines_dir.glob("*.yml")
        )
        for f in yaml_files:
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
            except Exception:
                results.append(
                    {
                        "name": f.stem,
                        "version": "?",
                        "description": "Error loading pipeline",
                        "steps": 0,
                        "path": str(f),
                    }
                )
        return results

    def list_bricks(self) -> list[dict]:
        """List all available brick definitions."""
        if not self.bricks_dir.exists():
            return []

        results = []
        yaml_files = sorted(self.bricks_dir.glob("*.yaml")) + sorted(
            self.bricks_dir.glob("*.yml")
        )
        for f in yaml_files:
            try:
                data = yaml.safe_load(f.read_text()) or {}
                results.append(
                    {
                        "name": data.get("name", f.stem),
                        "type": data.get("type", "?"),
                        "description": data.get("description", ""),
                        "version": data.get("version", "?"),
                        "tested": data.get("tested", False),
                    }
                )
            except Exception:
                results.append(
                    {
                        "name": f.stem,
                        "type": "?",
                        "description": "Error",
                        "version": "?",
                        "tested": False,
                    }
                )
        return results

    def get_pipeline_info(self, name: str) -> Optional[dict]:
        """Get detailed info about a pipeline."""
        if not self.pipelines_dir.exists():
            return None

        yaml_files = list(self.pipelines_dir.glob("*.yaml")) + list(
            self.pipelines_dir.glob("*.yml")
        )
        for f in yaml_files:
            try:
                pipeline = self.loader.load(str(f))
                if pipeline.name == name or f.stem == name:
                    return {
                        "name": pipeline.name,
                        "version": pipeline.version,
                        "description": pipeline.description,
                        "input": {
                            k: {
                                "type": v.type,
                                "default": v.default,
                                "description": v.description,
                            }
                            for k, v in pipeline.input.items()
                        },
                        "credentials": list(pipeline.credentials.keys()),
                        "steps": [{"id": s.id, "type": s.type} for s in pipeline.steps],
                        "mcp_servers": list(
                            {
                                s.server
                                for s in pipeline.steps
                                if s.type == "mcp" and s.server
                            }
                        ),
                        "path": str(f),
                    }
            except Exception:
                continue
        return None
