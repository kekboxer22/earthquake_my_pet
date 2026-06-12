import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

#  DAG Config
OWNER = "kek_prgraamer"
DAG_ID = "raw_from_s3_to_pg"

# Tables used in DAG
LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# DuckDB
PASSWORD = Variable.get("pg_password")
LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 6, 1),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_raw_data_to_ods_pg(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

    con.sql("SELECT current_database()").show()

    con.sql(f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
    """)

    # Создаём секрет для PostgreSQL
    con.sql(f"""
        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{PASSWORD}'
        );
    """)

    # Диагностика: проверим, создался ли секрет
    con.sql("SELECT * FROM duckdb_secrets()").show()

    # Пробуем прикрепить PostgreSQL
    con.sql("ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);")

    # Диагностика: какие таблицы видны после ATTACH
    con.sql("SHOW ALL TABLES").show()

    # Проверим, что таблица в PostgreSQL существует (создадим, если нет)
    con.sql(f"""
        CREATE SCHEMA IF NOT EXISTS dwh_postgres_db.{SCHEMA};
        CREATE TABLE IF NOT EXISTS dwh_postgres_db.{SCHEMA}.{TARGET_TABLE} (
            time VARCHAR,
            latitude VARCHAR,
            longitude VARCHAR,
            depth VARCHAR,
            mag VARCHAR,
            mag_type VARCHAR,
            nst VARCHAR,
            gap VARCHAR,
            dmin VARCHAR,
            rms VARCHAR,
            net VARCHAR,
            id VARCHAR,
            updated VARCHAR,
            place VARCHAR,
            type VARCHAR,
            horizontal_error VARCHAR,
            depth_error VARCHAR,
            mag_error VARCHAR,
            mag_nst VARCHAR,
            status VARCHAR,
            location_source VARCHAR,
            mag_source VARCHAR
        );
    """)

    # Вставка данных
    con.sql(f"""
        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        SELECT
            time,
            latitude,
            longitude,
            depth,
            mag,
            magType AS mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontalError AS horizontal_error,
            depthError AS depth_error,
            magError AS mag_error,
            magNst AS mag_nst,
            status,
            locationSource AS location_source,
            magSource AS mag_source
        FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
    """)

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule="0 5 * * *",
    default_args=args,
    tags=["s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )



get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

end = EmptyOperator(
        task_id="end",
    )
start  >> get_and_transfer_raw_data_to_ods_pg >> end
#start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end
