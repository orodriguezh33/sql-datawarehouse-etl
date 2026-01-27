from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Sequence

from .config import SqlServerSettings


class CommandExecutionError(RuntimeError):
    pass


@dataclass(frozen=True)
class SqlCmdResult:
    returncode: int
    stdout: str
    stderr: str


class DockerSqlCmdRunner:
    """
    Ejecuta sqlcmd dentro de un contenedor Docker.
    SRP: solo sabe construir y ejecutar comandos.
    """

    def __init__(self, settings: SqlServerSettings) -> None:
        self._s = settings

    def run_sql_file(self, relative_path: str, database: str) -> SqlCmdResult:
        cmd = self._base_cmd(database)
        cmd += ["-i", f"{self._s.sql_base_path}/{relative_path}"]
        return self._run(cmd)

    def run_query(self, query: str, database: str) -> SqlCmdResult:
        cmd = self._base_cmd(database)
        cmd += ["-Q", query]
        return self._run(cmd)

    def _base_cmd(self, database: str) -> list[str]:
        return [
            "docker",
            "exec",
            "-i",
            self._s.container_name,
            self._s.sqlcmd_path,
            "-S",
            f"{self._s.host},{self._s.port}",
            "-U",
            self._s.user,
            "-P",
            self._s.password,
            "-d",
            database,
            "-C",  # trust server cert
            "-b",  # fail on error
        ]

    def _run(self, cmd: Sequence[str]) -> SqlCmdResult:
        proc = subprocess.run(
            list(cmd),
            check=False,
            capture_output=True,
            text=True,
        )

        result = SqlCmdResult(
            returncode=proc.returncode,
            stdout=proc.stdout or "",
            stderr=proc.stderr or "",
        )

        if proc.returncode != 0:
            raise CommandExecutionError(
                "Error ejecutando sqlcmd.\n"
                f"CMD: {' '.join(cmd)}\n"
                f"STDOUT:\n{result.stdout}\n"
                f"STDERR:\n{result.stderr}"
            )

        return result
