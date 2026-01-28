"""ETL pipeline primitives: steps and pipeline runner.

This module defines:
- Step protocol (a contract for pipeline steps)
- Concrete steps (SQL file and raw query)
- EtlPipeline orchestrator that runs steps in order
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

from .runners import DockerSqlCmdRunner


class Step(Protocol):
    """A minimal contract for pipeline steps."""

    @property
    def name(self) -> str: ...

    def run(self) -> None: ...

    """Execute the step."""


@dataclass(frozen=True)
class SqlFileStep:
    """Pipeline step that executes a SQL file using the provided runner."""

    name: str
    runner: DockerSqlCmdRunner
    relative_path: str
    database: str

    def run(self) -> None:
        """Run the SQL file step."""
        self.runner.run_sql_file(self.relative_path, self.database)


@dataclass(frozen=True)
class QueryStep:
    """Pipeline step that executes a raw SQL query using the provided runner."""

    name: str
    runner: DockerSqlCmdRunner
    query: str
    database: str

    def run(self) -> None:
        """Run the query step."""
        self.runner.run_query(self.query, self.database)


class EtlPipeline:
    """Runs a list of steps in order.

    Design note:
        The pipeline is open for extension: you can add new step types
        (e.g., PythonStep, ApiStep) without changing this class.
    """

    def __init__(self, steps: Iterable[Step]) -> None:
        self._steps = list(steps)

    def run(self) -> None:
        """Execute all steps in order."""
        for step in self._steps:
            print(f"\n▶ {step.name}")
            step.run()

        print("\n✅ ETL end-to-end executed successfully")
