from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class SqlServerSettings:
    host: str
    port: str
    user: str
    password: str
    default_db: str
    container_name: str
    sql_base_path: str
    sqlcmd_path: str

    @classmethod
    def from_env(cls) -> "SqlServerSettings":
        load_dotenv()

        host = os.getenv("MSSQL_HOST", "localhost")
        port = os.getenv("MSSQL_PORT", "1433")
        user = os.getenv("MSSQL_USER", "sa")
        password = os.getenv("MSSQL_SA_PASSWORD", "")
        default_db = os.getenv("MSSQL_DB", "master")

        if not password:
            raise ValueError("MSSQL_SA_PASSWORD no está definido en tu .env")

        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            default_db=default_db,
            container_name=os.getenv("MSSQL_CONTAINER", "sqlserver_dwh"),
            sql_base_path=os.getenv("SQL_BASE_PATH", "/sql"),
            sqlcmd_path=os.getenv("SQLCMD_PATH", "/opt/mssql-tools18/bin/sqlcmd"),
        )
