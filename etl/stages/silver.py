from __future__ import annotations

from etl.checks.sql_generators import build_fail_if_zero_sql, build_row_counts_sql
from etl.pipeline import QueryStep, SqlFileStep, Step
from etl.runners import DockerSqlCmdRunner

SILVER_TABLES = [
    "silver.crm_cust_info",
    "silver.crm_prd_info",
    "silver.crm_sales_details",
    "silver.erp_loc_a101",
    "silver.erp_cust_az12",
    "silver.erp_px_cat_g1v2",
]


def build_silver_steps(runner: DockerSqlCmdRunner, dw: str) -> list[Step]:
    steps: list[Step] = [
        SqlFileStep(
            "Create Silver Tables", runner, "20_silver/00_create_silver_tables.sql", dw
        ),
        SqlFileStep(
            "Create/Update silver.load_silver",
            runner,
            "20_silver/10_load_silver.sql",
            dw,
        ),
        QueryStep("Execute silver.load_silver", runner, "EXEC silver.load_silver;", dw),
        QueryStep("Silver row counts", runner, build_row_counts_sql(SILVER_TABLES), dw),
        QueryStep(
            "Silver fail if zero",
            runner,
            build_fail_if_zero_sql(SILVER_TABLES, 51001),
            dw,
        ),
    ]
    return steps
