"""Gold stage steps.

This module builds the list of pipeline steps required to create the Gold layer
(analytics-ready views) and validate that the views return data.
"""

from __future__ import annotations

from typing import List

from ..checks.sql_generators import build_fail_if_zero_sql, build_row_counts_sql
from ..pipeline import QueryStep, SqlFileStep, Step
from ..runners import SqlCmdRunner

# The views expected to be created by the SQL files
GOLD_VIEWS: tuple[str, ...] = (
    "gold.dim_customers",
    "gold.dim_products",
    "gold.fact_sales",
    "gold.report_customers",
    "gold.report_products",
)

GOLD_ERROR_BASE = 52001


def build_gold_steps(runner: SqlCmdRunner, database: str) -> List[Step]:
    """Build pipeline steps for the Gold stage."""

    # 1. Define the list of SQL files in your 30_gold folder
    # We maintain the order of execution: first base views, then reports
    gold_sql_files = [
        "30_gold/00_create_gold_views.sql",  # Contains dim_customers, dim_products, fact_sales
        "30_gold/10_create_report_customers.sql",  # Contains report_customers
        "30_gold/20_create_report_products.sql",  # Contains report_products
    ]

    # 2. Generate steps dynamically
    # Each file becomes a unique SqlFileStep for better logging and error tracking
    steps: List[Step] = []

    for file_path in gold_sql_files:
        # Extract filename for a cleaner step name (e.g., '01_create_report_customers.sql')
        step_label = file_path.split("/")[-1]

        steps.append(
            SqlFileStep(
                name=f"Executing: {step_label}",
                runner=runner,
                relative_path=file_path,
                database=database,
            )
        )

    # 3. Add validation steps after all views have been created
    steps.extend(
        [
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
    )

    return steps
