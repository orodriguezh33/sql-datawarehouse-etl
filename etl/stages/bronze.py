from __future__ import annotations

from etl.checks.sql_generators import build_fail_if_zero_sql, build_row_counts_sql
from etl.pipeline import QueryStep, SqlFileStep, Step
from etl.runners import DockerSqlCmdRunner

BRONZE_TABLES = [
    "bronze.crm_cust_info",
    "bronze.crm_prd_info",
    "bronze.crm_sales_details",
    "bronze.erp_loc_a101",
    "bronze.erp_cust_az12",
    "bronze.erp_px_cat_g1v2",
]


def build_bronze_steps(runner: DockerSqlCmdRunner, dw: str) -> list[Step]:
    steps: list[Step] = [
        SqlFileStep(
            "Create Bronze Tables", runner, "10_bronze/00_create_bronze_tables.sql", dw
        ),
        SqlFileStep("Load Bronze (script)", runner, "10_bronze/10_load_bronze.sql", dw),
        QueryStep(
            "Run bronze.load_bronze + sample",
            runner,
            "EXEC bronze.load_bronze; SELECT TOP 10 * FROM bronze.crm_cust_info;",
            dw,
        ),
        QueryStep("Bronze row counts", runner, build_row_counts_sql(BRONZE_TABLES), dw),
        QueryStep(
            "Bronze fail if zero",
            runner,
            build_fail_if_zero_sql(BRONZE_TABLES, 50001),
            dw,
        ),
    ]
    return steps
