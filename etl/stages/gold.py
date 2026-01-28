from __future__ import annotations

from etl.checks.sql_generators import build_fail_if_zero_sql, build_row_counts_sql
from etl.pipeline import QueryStep, SqlFileStep, Step
from etl.runners import DockerSqlCmdRunner

GOLD_VIEWS = [
    "gold.dim_customers",
    "gold.dim_products",
    "gold.fact_sales",
]


def build_gold_steps(runner: DockerSqlCmdRunner, dw: str) -> list[Step]:
    steps: list[Step] = [
        # Build: crea/actualiza vistas (archivo .sql con GO => debe ser SqlFileStep)
        SqlFileStep(
            "Create Gold Views",
            runner,
            "30_gold/00_create_gold_views.sql",
            dw,
        ),
        # Checks
        QueryStep("Gold view row counts", runner, build_row_counts_sql(GOLD_VIEWS), dw),
        QueryStep(
            "Gold views fail if zero",
            runner,
            build_fail_if_zero_sql(GOLD_VIEWS, 52001),
            dw,
        ),
    ]
    return steps
