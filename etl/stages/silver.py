"""Silver stage steps.

This module builds the list of pipeline steps required to populate the Silver
layer (cleansed/transformed data) and validate that it contains data.
"""

from __future__ import annotations

from typing import List

from ..checks.sql_generators import build_fail_if_zero_sql, build_row_counts_sql
from ..pipeline import QueryStep, SqlFileStep, Step
from ..runners import SqlCmdRunner

SILVER_TABLES: tuple[str, ...] = (
    "silver.crm_cust_info",
    "silver.crm_prd_info",
    "silver.crm_sales_details",
    "silver.erp_loc_a101",
    "silver.erp_cust_az12",
    "silver.erp_px_cat_g1v2",
)

SILVER_ERROR_BASE = 51001


def build_silver_steps(runner: SqlCmdRunner, database: str) -> List[Step]:
    """Build pipeline steps for the Silver stage.

    Args:
        runner: Runner responsible for executing SQL commands.
        database: Target database name (e.g., "DataWarehouse").

    Returns:
        A list of pipeline steps for the Silver stage.
    """
    steps: List[Step] = [
        SqlFileStep(
            name="Create Silver Tables",
            runner=runner,
            relative_path="20_silver/00_create_silver_tables.sql",
            database=database,
        ),
        SqlFileStep(
            name="Create/Update silver.load_silver",
            runner=runner,
            relative_path="20_silver/10_load_silver.sql",
            database=database,
        ),
        QueryStep(
            name="Execute silver.load_silver",
            runner=runner,
            query="EXEC silver.load_silver;",
            database=database,
        ),
        QueryStep(
            name="Silver row counts",
            runner=runner,
            query=build_row_counts_sql(SILVER_TABLES),
            database=database,
        ),
        QueryStep(
            name="Fail if any Silver table is empty",
            runner=runner,
            query=build_fail_if_zero_sql(SILVER_TABLES, error_base=SILVER_ERROR_BASE),
            database=database,
        ),
    ]
    return steps
