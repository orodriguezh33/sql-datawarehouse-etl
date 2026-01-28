"""Gold stage steps.

This module builds the list of pipeline steps required to create the Gold layer
(analytics-ready views) and validate that the views return data.
"""

from __future__ import annotations

from typing import List

from ..checks.sql_generators import build_fail_if_zero_sql, build_row_counts_sql
from ..pipeline import QueryStep, SqlFileStep, Step
from ..runners import DockerSqlCmdRunner

GOLD_VIEWS: tuple[str, ...] = (
    "gold.dim_customers",
    "gold.dim_products",
    "gold.fact_sales",
)

GOLD_ERROR_BASE = 52001


def build_gold_steps(runner: DockerSqlCmdRunner, database: str) -> List[Step]:
    """Build pipeline steps for the Gold stage.

    Notes:
        Gold objects are implemented as views. If the SQL file contains `GO`,
        it must be executed as a file (sqlcmd -i), not as an inline query.

    Args:
        runner: Runner responsible for executing SQL commands.
        database: Target database name (e.g., "DataWarehouse").

    Returns:
        A list of pipeline steps for the Gold stage.
    """
    steps: List[Step] = [
        SqlFileStep(
            name="Create Gold Views",
            runner=runner,
            relative_path="30_gold/00_create_gold_views.sql",
            database=database,
        ),
        QueryStep(
            name="Gold view row counts",
            runner=runner,
            query=build_row_counts_sql(GOLD_VIEWS),
            database=database,
        ),
        QueryStep(
            name="Fail if any Gold view is empty",
            runner=runner,
            query=build_fail_if_zero_sql(GOLD_VIEWS, error_base=GOLD_ERROR_BASE),
            database=database,
        ),
    ]
    return steps
