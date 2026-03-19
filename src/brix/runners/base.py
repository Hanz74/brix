"""Base runner interface — all runners implement this."""

from abc import ABC, abstractmethod
from typing import Any


class BaseRunner(ABC):
    """Base interface for all pipeline step runners."""

    @abstractmethod
    async def execute(self, step: Any, context: Any) -> dict:
        """Execute a pipeline step.

        Args:
            step: Step configuration (Step model or dict)
            context: Pipeline execution context

        Returns:
            dict with keys: success (bool), data (Any), duration (float)
            On error: success=False, error=str
        """
        ...
