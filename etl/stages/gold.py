from __future__ import annotations

from etl.checks.sql_generators import build_fail_if_zero_sql, build_row_counts_sql
from etl.pipeline import QueryStep, Step
from etl.runners import DockerSqlCmdRunner

GOLD_TABLES = [
    "gold.sales_summary_daily",
    "gold.customer_rfm",
]


def build_gold_steps(runner: DockerSqlCmdRunner, dw: str) -> list[Step]:
    steps: list[Step] = [
        # cuando tengas scripts en sql/30_gold, los llamas aquí
        # SqlFileStep("Create Gold Tables", runner, "30_gold/00_create_gold_tables.sql", dw),
        QueryStep("Load Gold", runner, "EXEC gold.load_gold;", dw),
        QueryStep("Gold row counts", runner, build_row_counts_sql(GOLD_TABLES), dw),
        QueryStep(
            "Gold fail if zero", runner, build_fail_if_zero_sql(GOLD_TABLES, 52001), dw
        ),
    ]
    return steps
