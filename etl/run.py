import os
import subprocess

from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("MSSQL_HOST", "localhost")
PORT = os.getenv("MSSQL_PORT", "1433")
USER = os.getenv("MSSQL_USER", "sa")
PWD = os.getenv("MSSQL_SA_PASSWORD")
DB = os.getenv("MSSQL_DB", "master")

SQL_BASE = "/sql"  # viene del volumen ./sql:/sql


def run_sql_file(relative_path: str, database: str):
    """Ejecuta un archivo .sql montado dentro del contenedor en /sql/..."""
    cmd = [
        "docker",
        "exec",
        "-i",
        "sqlserver_dwh",
        "/opt/mssql-tools18/bin/sqlcmd",
        "-S",
        f"{HOST},{PORT}",
        "-U",
        USER,
        "-P",
        PWD,
        "-d",
        database,
        "-C",  # trust server cert
        "-b",  # fail on error
        "-i",
        f"{SQL_BASE}/{relative_path}",
    ]
    subprocess.run(cmd, check=True)


def run_query(query: str, database: str = "DataWarehouse"):
    """Ejecuta un query inline con -Q"""
    cmd = [
        "docker",
        "exec",
        "sqlserver_dwh",
        "/opt/mssql-tools18/bin/sqlcmd",
        "-S",
        f"{HOST},{PORT}",
        "-U",
        USER,
        "-P",
        PWD,
        "-d",
        database,
        "-C",
        "-b",
        "-Q",
        query,
    ]
    subprocess.run(cmd, check=True)


def main():
    # 1) Setup / DDL
    run_sql_file("00_setup/00_create_db.sql", "master")
    run_sql_file("00_setup/01_create_schemas.sql", "DataWarehouse")
    run_sql_file("10_bronze/00_create_bronze_tables.sql", "DataWarehouse")
    run_sql_file("10_bronze/10_load_bronze.sql", "DataWarehouse")

    run_sql_file("20_silver/00_create_silver_tables.sql", "DataWarehouse")

    # 2) Cargar bronze + muestra rápida
    run_query("EXEC bronze.load_bronze; SELECT TOP 10 * FROM bronze.crm_cust_info;")

    # 3) Conteos por tabla (comprobación)
    summary_query = """
    SELECT 'crm_cust_info' AS tbl, COUNT(*) AS rows FROM bronze.crm_cust_info
    UNION ALL SELECT 'crm_prd_info', COUNT(*) FROM bronze.crm_prd_info
    UNION ALL SELECT 'crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
    UNION ALL SELECT 'erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101
    UNION ALL SELECT 'erp_cust_az12', COUNT(*) FROM bronze.erp_cust_az12
    UNION ALL SELECT 'erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2;
    """
    print("\n=== Bronze row counts ===")
    run_query(summary_query)

    # 4) Validación mínima: si una tabla queda en 0, fallar (evita “ETL verde” con carga vacía)
    fail_if_zero = """
    IF (SELECT COUNT(*) FROM bronze.crm_cust_info) = 0 THROW 50001, 'bronze.crm_cust_info loaded 0 rows', 1;
    IF (SELECT COUNT(*) FROM bronze.crm_prd_info) = 0 THROW 50002, 'bronze.crm_prd_info loaded 0 rows', 1;
    IF (SELECT COUNT(*) FROM bronze.crm_sales_details) = 0 THROW 50003, 'bronze.crm_sales_details loaded 0 rows', 1;
    IF (SELECT COUNT(*) FROM bronze.erp_loc_a101) = 0 THROW 50004, 'bronze.erp_loc_a101 loaded 0 rows', 1;
    IF (SELECT COUNT(*) FROM bronze.erp_cust_az12) = 0 THROW 50005, 'bronze.erp_cust_az12 loaded 0 rows', 1;
    IF (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2) = 0 THROW 50006, 'bronze.erp_px_cat_g1v2 loaded 0 rows', 1;
    """
    run_query(fail_if_zero)

    print("\n✅ ETL end-to-end ejecutado correctamente")


if __name__ == "__main__":
    if not PWD:
        raise ValueError("MSSQL_SA_PASSWORD no está definido en tu .env")
    main()
