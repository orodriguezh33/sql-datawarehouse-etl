from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

from .runners import DockerSqlCmdRunner


class Step(Protocol):
    name: str

    def run(self) -> None: ...


@dataclass(frozen=True)
class SqlFileStep:
    name: str
    runner: DockerSqlCmdRunner
    relative_path: str
    database: str

    def run(self) -> None:
        self.runner.run_sql_file(self.relative_path, self.database)


@dataclass(frozen=True)
class QueryStep:
    name: str
    runner: DockerSqlCmdRunner
    query: str
    database: str

    def run(self) -> None:
        self.runner.run_query(self.query, self.database)


class EtlPipeline:
    """
    OCP: puedes agregar pasos nuevos sin tocar la lógica del runner.
    """

    def __init__(self, steps: Iterable[Step]) -> None:
        self._steps = list(steps)

    def run(self) -> None:
        for step in self._steps:
            print(f"\n▶ {step.name}")
            step.run()
        print("\n✅ ETL end-to-end ejecutado correctamente")
