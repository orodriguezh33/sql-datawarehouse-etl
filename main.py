from etl.config import SqlServerSettings
from etl.pipeline import EtlPipeline, QueryStep, SqlFileStep
from etl.runners import DockerSqlCmdRunner


def build_pipeline(runner: DockerSqlCmdRunner) -> EtlPipeline:
    dw = "DataWarehouse"

    summary_query = """
    SELECT 'crm_cust_info' AS tbl, COUNT(*) AS rows FROM bronze.crm_cust_info
    UNION ALL SELECT 'crm_prd_info', COUNT(*) FROM bronze.crm_prd_info
    UNION ALL SELECT 'crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
    UNION ALL SELECT 'erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101
    UNION ALL SELECT 'erp_cust_az12', COUNT(*) FROM bronze.erp_cust_az12
    UNION ALL SELECT 'erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2;
    """

    fail_if_zero = """
    IF (SELECT COUNT(*) FROM bronze.crm_cust_info) = 0 THROW 50001, 'bronze.crm_cust_info loaded 0 rows', 1;
    IF (SELECT COUNT(*) FROM bronze.crm_prd_info) = 0 THROW 50002, 'bronze.crm_prd_info loaded 0 rows', 1;
    IF (SELECT COUNT(*) FROM bronze.crm_sales_details) = 0 THROW 50003, 'bronze.crm_sales_details loaded 0 rows', 1;
    IF (SELECT COUNT(*) FROM bronze.erp_loc_a101) = 0 THROW 50004, 'bronze.erp_loc_a101 loaded 0 rows', 1;
    IF (SELECT COUNT(*) FROM bronze.erp_cust_az12) = 0 THROW 50005, 'bronze.erp_cust_az12 loaded 0 rows', 1;
    IF (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2) = 0 THROW 50006, 'bronze.erp_px_cat_g1v2 loaded 0 rows', 1;
    """

    steps = [
        # 1) Setup / DDL
        SqlFileStep("Create DB", runner, "00_setup/00_create_db.sql", "master"),
        SqlFileStep("Create Schemas", runner, "00_setup/01_create_schemas.sql", dw),
        SqlFileStep(
            "Create Bronze Tables", runner, "10_bronze/00_create_bronze_tables.sql", dw
        ),
        SqlFileStep("Load Bronze (script)", runner, "10_bronze/10_load_bronze.sql", dw),
        SqlFileStep(
            "Create Silver Tables", runner, "20_silver/00_create_silver_tables.sql", dw
        ),
        # 2) Cargar bronze + muestra rápida
        QueryStep(
            "Run bronze.load_bronze + sample",
            runner,
            "EXEC bronze.load_bronze; SELECT TOP 10 * FROM bronze.crm_cust_info;",
            dw,
        ),
        # 3) Conteos
        QueryStep("Bronze row counts", runner, summary_query, dw),
        # 4) Validación mínima
        QueryStep("Fail if any bronze table is empty", runner, fail_if_zero, dw),
    ]

    return EtlPipeline(steps)


def main() -> None:
    settings = SqlServerSettings.from_env()
    runner = DockerSqlCmdRunner(settings)
    pipeline = build_pipeline(runner)
    pipeline.run()


if __name__ == "__main__":
    main()
