"""SQL command runner using sqlcmd over TCP (no docker exec).

This runner is designed to work both:
- Locally (if you have sqlcmd installed), and
- Inside a Docker container (recommended for this project).

It connects to SQL Server via network using:
    sqlcmd -S <host>,<port> -U <user> -P <password> -d <db> ...

This avoids requiring the Docker CLI inside the ETL container.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from typing import Sequence

from .config import SqlServerSettings


class CommandExecutionError(RuntimeError):
    """Raised when a sqlcmd command execution fails."""


@dataclass(frozen=True)
class SqlCmdResult:
    """Result of a sqlcmd execution."""

    returncode: int
    stdout: str
    stderr: str


class SqlCmdRunner:
    """Execute SQL Server commands via sqlcmd.

    Responsibility (SRP):
        - Build sqlcmd commands
        - Execute them
        - Capture stdout/stderr
        - Raise a clear error if something fails
    """

    def __init__(self, settings: SqlServerSettings) -> None:
        self._settings = settings

    def run_sql_file(self, relative_path: str, database: str) -> SqlCmdResult:
        """Execute a .sql file (mounted at SQL_BASE_PATH) on the target database."""
        command = self._build_base_command(database)

        sql_file_path = os.path.join(self._settings.sql_base_path, relative_path)
        command.extend(["-i", sql_file_path])

        return self._run(command)

    def run_query(self, query: str, database: str) -> SqlCmdResult:
        """Execute a raw SQL query on the target database."""
        command = self._build_base_command(database)
        command.extend(["-Q", query])
        return self._run(command)

    def _build_base_command(self, database: str) -> list[str]:
        """Build the base sqlcmd command."""
        return [
            self._settings.sqlcmd_path,
            "-S",
            f"{self._settings.host},{self._settings.port}",
            "-U",
            self._settings.user,
            "-P",
            self._settings.password,
            "-d",
            database,
            "-C",  # Trust server certificate
            "-b",  # Fail on SQL errors (non-zero exit code)
        ]

    def _run(self, command: Sequence[str]) -> SqlCmdResult:
        """Run the command and validate the result."""
        process = subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            text=True,
        )

        result = SqlCmdResult(
            returncode=process.returncode,
            stdout=process.stdout or "",
            stderr=process.stderr or "",
        )

        if result.returncode != 0:
            raise CommandExecutionError(
                "sqlcmd execution failed.\n"
                f"Command: {' '.join(command)}\n"
                f"STDOUT:\n{result.stdout}\n"
                f"STDERR:\n{result.stderr}"
            )

        return result
