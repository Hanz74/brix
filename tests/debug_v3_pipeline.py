import asyncio
import logging
import sys
import traceback
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from brix.engine import PipelineEngine
from brix.pipeline_store import PipelineStore


async def main() -> None:
    logging.basicConfig(
        level=logging.ERROR,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    store = PipelineStore()
    pipeline = store.load("buddy-process-attachments-v3")
    user_input = {"source_pattern": "onedrive%", "limit": 1}

    print(f"Loaded pipeline: {pipeline.name}")
    print(f"User input: {user_input}")
    print("Defined steps:")
    for idx, step in enumerate(pipeline.steps, 1):
        print(f"  {idx}. {step.id} [{step.type}]")

    engine = PipelineEngine()

    try:
        result = await engine.run(pipeline, user_input=user_input)
    except Exception:
        print("ENGINE RAISED EXCEPTION")
        print(traceback.format_exc())
        raise

    print(f"Run success: {result.success}")
    print(f"Final output: {result.output!r}")
    print("Step statuses:")
    for idx, step in enumerate(pipeline.steps, 1):
        status = result.steps.get(step.id)
        if status is None:
            print(f"  {idx}. {step.id}: <missing>")
            continue
        print(
            f"  {idx}. {step.id}: status={status.status!r} "
            f"duration={status.duration!r} errors={status.errors!r} "
            f"reason={status.reason!r} error_message={status.error_message!r}"
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        print("TOP-LEVEL EXCEPTION")
        print(traceback.format_exc())
        raise
