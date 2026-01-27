from __future__ import annotations

from typing import Iterable


def build_row_counts_sql(tables: Iterable[str]) -> str:
    tables = list(tables)
    if not tables:
        raise ValueError("tables vacío")

    parts = []
    for i, tbl in enumerate(tables):
        prefix = "SELECT" if i == 0 else "UNION ALL SELECT"
        parts.append(f"{prefix} '{tbl}' AS tbl, COUNT(*) AS rows FROM {tbl}")
    return "\n".join(parts) + ";"


def build_fail_if_zero_sql(tables: Iterable[str], error_base: int = 50001) -> str:
    lines = []
    for i, tbl in enumerate(tables):
        err = error_base + i
        lines.append(
            f"IF (SELECT COUNT(*) FROM {tbl}) = 0 "
            f"THROW {err}, '{tbl} loaded 0 rows', 1;"
        )
    return "\n".join(lines)
