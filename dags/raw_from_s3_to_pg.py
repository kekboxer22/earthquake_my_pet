import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Config
OWNER = "kek_prgraamer"
DAG_ID = "raw_from_s3_to_pg"
LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# DuckDB
PASSWORD = Variable.get("pg_password")

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 6, 1),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date

def get_and_transfer_raw_data_to_ods_pg(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

    # Читаем Parquet из MinIO
    con.execute(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
        CREATE TEMP TABLE raw_data AS
        SELECT
            time, latitude, longitude, depth, mag,
            magType AS mag_type, nst, gap, dmin, rms,
            net, id, updated, place, type,
            horizontalError AS horizontal_error,
            depthError AS depth_error,
            magError AS mag_error,
            magNst AS mag_nst, status,
            locationSource AS location_source,
            magSource AS mag_source
        FROM 's3://prod/raw/earthquake/{start_date}/{start_date}_00-00-00.gz.parquet';
    """)

    # Проверка, что данные прочитались
    count = con.execute("SELECT COUNT(*) FROM raw_data").fetchone()[0]
    logging.info(f"Read {count} rows from MinIO for {start_date}")

    # Вставляем в PostgreSQL через postgres_scan
    con.execute(f"""
        INSERT INTO postgres_scan('postgres_dwh', 5432, 'postgres', 'postgres', '{PASSWORD}', 'ods.fct_earthquake')
        SELECT * FROM raw_data;
    """)

    con.close()
    logging.info(f"✅ Data for {start_date} loaded successfully")

with DAG(
    dag_id=DAG_ID,
    schedule="0 5 * * *",
    default_args=args,
    tags=["s3", "ods", "pg"],
    description="Load data from MinIO to PostgreSQL",
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    start = EmptyOperator(task_id="start")
    load_task = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )
    end = EmptyOperator(task_id="end")
    start >> load_task >> end
