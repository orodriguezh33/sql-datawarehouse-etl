"""SQL generators for simple data quality checks."""

from __future__ import annotations

from typing import Iterable


def build_row_counts_sql(tables: Iterable[str]) -> str:
    """Build a SQL query that returns row counts for each provided table.

    The resulting SQL returns two columns:
        - tbl: table name (string)
        - rows: row count (integer)

    Args:
        tables: An iterable of fully-qualified table names.

    Returns:
        A SQL string with one SELECT per table combined using UNION ALL.

    Raises:
        ValueError: If no tables are provided.
    """
    table_list = list(tables)
    if not table_list:
        raise ValueError("No tables provided for row count check.")

    sql_parts: list[str] = []
    for index, table_name in enumerate(table_list):
        prefix = "SELECT" if index == 0 else "UNION ALL SELECT"
        sql_parts.append(
            f"{prefix} '{table_name}' AS tbl, COUNT(*) AS rows FROM {table_name}"
        )

    return "\n".join(sql_parts) + ";"


def build_fail_if_zero_sql(tables: Iterable[str], error_base: int = 50001) -> str:
    """Build SQL statements that fail if any table has zero rows.

    For each table, the SQL will execute:
        IF (SELECT COUNT(*) FROM <table>) = 0 THROW <error>, '<table> loaded 0 rows', 1;

    Error numbers are generated sequentially:
        error_base, error_base + 1, error_base + 2, ...

    Args:
        tables: An iterable of fully-qualified table names.
        error_base: Base SQL Server error number (must be >= 50000 for THROW).

    Returns:
        A SQL string containing one validation statement per table.

    Raises:
        ValueError: If no tables are provided.
    """
    table_list = list(tables)
    if not table_list:
        raise ValueError("No tables provided for zero-row validation.")

    statements: list[str] = []
    for index, table_name in enumerate(table_list):
        error_number = error_base + index
        statements.append(
            f"IF (SELECT COUNT(*) FROM {table_name}) = 0 "
            f"THROW {error_number}, '{table_name} loaded 0 rows', 1;"
        )

    return "\n".join(statements)
