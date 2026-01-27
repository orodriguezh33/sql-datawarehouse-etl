from etl.config import SqlServerSettings
from etl.pipeline import EtlPipeline, SqlFileStep
from etl.runners import DockerSqlCmdRunner
from etl.stages.bronze import build_bronze_steps
from etl.stages.gold import build_gold_steps
from etl.stages.silver import build_silver_steps


def build_pipeline(runner: DockerSqlCmdRunner) -> EtlPipeline:
    dw = "DataWarehouse"

    steps = [
        SqlFileStep("Create DB", runner, "00_setup/00_create_db.sql", "master"),
        SqlFileStep("Create Schemas", runner, "00_setup/01_create_schemas.sql", dw),
        *build_bronze_steps(runner, dw),
        *build_silver_steps(runner, dw),
        *build_gold_steps(runner, dw),
    ]
    return EtlPipeline(steps)


def main() -> None:
    settings = SqlServerSettings.from_env()
    runner = DockerSqlCmdRunner(settings)
    build_pipeline(runner).run()


if __name__ == "__main__":
    main()
