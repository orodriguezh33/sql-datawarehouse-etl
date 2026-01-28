"""Application entry point for running the end-to-end SQL Server ETL pipeline."""

from etl.config import SqlServerSettings
from etl.pipeline import EtlPipeline, SqlFileStep
from etl.runners import DockerSqlCmdRunner
from etl.stages.bronze import build_bronze_steps
from etl.stages.gold import build_gold_steps
from etl.stages.silver import build_silver_steps

DATA_WAREHOUSE_DB = "DataWarehouse"
MASTER_DB = "master"


def build_pipeline(runner: DockerSqlCmdRunner) -> EtlPipeline:
    """Build the ETL pipeline steps in execution order.

    Args:
        runner: Runner responsible for executing SQL files against SQL Server.

    Returns:
        A configured EtlPipeline instance ready to be executed.
    """
    steps = [
        SqlFileStep("Create DB", runner, "00_setup/00_create_db.sql", MASTER_DB),
        SqlFileStep(
            "Create Schemas",
            runner,
            "00_setup/01_create_schemas.sql",
            DATA_WAREHOUSE_DB,
        ),
        *build_bronze_steps(runner, DATA_WAREHOUSE_DB),
        *build_silver_steps(runner, DATA_WAREHOUSE_DB),
        *build_gold_steps(runner, DATA_WAREHOUSE_DB),
    ]
    return EtlPipeline(steps)


def main() -> None:
    """Load settings, create the SQL runner, and execute the ETL pipeline."""
    settings = SqlServerSettings.from_env()
    runner = DockerSqlCmdRunner(settings)

    pipeline = build_pipeline(runner)
    pipeline.run()


if __name__ == "__main__":
    main()
