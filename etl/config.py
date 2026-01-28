"""Configuration for SQL Server ETL execution.

This module loads environment variables (optionally from a .env file) and
builds a validated settings object used by the SQL runner.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class SqlServerSettings:
    """Holds SQL Server connection and execution settings."""

    host: str
    port: int
    user: str
    password: str
    default_db: str
    container_name: str
    sql_base_path: str
    sqlcmd_path: str

    @classmethod
    def from_env(cls) -> "SqlServerSettings":
        """Build settings from environment variables (and .env if present)."""
        load_dotenv(override=False)

        host = os.getenv("MSSQL_HOST", "localhost").strip()
        port_str = os.getenv("MSSQL_PORT", "1433").strip()
        user = os.getenv("MSSQL_USER", "sa").strip()
        password = os.getenv("MSSQL_SA_PASSWORD", "").strip()
        default_db = os.getenv("MSSQL_DB", "master").strip()

        container_name = os.getenv("MSSQL_CONTAINER", "sqlserver_dwh").strip()
        sql_base_path = os.getenv("SQL_BASE_PATH", "/sql").strip()
        sqlcmd_path = os.getenv("SQLCMD_PATH", "/opt/mssql-tools18/bin/sqlcmd").strip()

        if not password:
            raise ValueError("Missing required environment variable: MSSQL_SA_PASSWORD")

        if not host:
            raise ValueError("MSSQL_HOST cannot be empty")

        if not user:
            raise ValueError("MSSQL_USER cannot be empty")

        if not container_name:
            raise ValueError("MSSQL_CONTAINER cannot be empty")

        try:
            port = int(port_str)
        except ValueError as exc:
            raise ValueError(
                f"MSSQL_PORT must be an integer, got: {port_str!r}"
            ) from exc

        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            default_db=default_db,
            container_name=container_name,
            sql_base_path=sql_base_path,
            sqlcmd_path=sqlcmd_path,
        )
