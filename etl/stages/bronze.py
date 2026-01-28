"""Bronze stage steps.

This module builds the list of pipeline steps required to populate the Bronze
layer (raw ingestion) and validate that it contains data.
"""

from __future__ import annotations

from typing import List

from ..checks.sql_generators import build_fail_if_zero_sql, build_row_counts_sql
from ..pipeline import QueryStep, SqlFileStep, Step
from ..runners import DockerSqlCmdRunner

BRONZE_TABLES: tuple[str, ...] = (
    "bronze.crm_cust_info",
    "bronze.crm_prd_info",
    "bronze.crm_sales_details",
    "bronze.erp_loc_a101",
    "bronze.erp_cust_az12",
    "bronze.erp_px_cat_g1v2",
)


def build_bronze_steps(runner: DockerSqlCmdRunner, database: str) -> List[Step]:
    """Build pipeline steps for the Bronze stage.

    Args:
        runner: Runner responsible for executing SQL commands.
        database: Target database name (e.g., "DataWarehouse").

    Returns:
        A list of pipeline steps for the Bronze stage.
    """
    steps: List[Step] = [
        SqlFileStep(
            name="Create Bronze Tables",
            runner=runner,
            relative_path="10_bronze/00_create_bronze_tables.sql",
            database=database,
        ),
        SqlFileStep(
            name="Load Bronze (SQL script)",
            runner=runner,
            relative_path="10_bronze/10_load_bronze.sql",
            database=database,
        ),
        QueryStep(
            name="Run bronze.load_bronze",
            runner=runner,
            query="EXEC bronze.load_bronze;",
            database=database,
        ),
        QueryStep(
            name="Sample bronze.crm_cust_info (TOP 10)",
            runner=runner,
            query="SELECT TOP 10 * FROM bronze.crm_cust_info;",
            database=database,
        ),
        QueryStep(
            name="Bronze row counts",
            runner=runner,
            query=build_row_counts_sql(BRONZE_TABLES),
            database=database,
        ),
        QueryStep(
            name="Fail if any Bronze table is empty",
            runner=runner,
            query=build_fail_if_zero_sql(BRONZE_TABLES, error_base=50001),
            database=database,
        ),
    ]
    return steps
