from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.hooks.base import BaseHook
import subprocess

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Paths (adjust if needed)
PRODUCER_SCRIPT = "/opt/airflow/producer/weather_producer_once.py"
CONSUMER_SCRIPT = "/opt/airflow/consumer/consume_batch.py"
HIVE_HQL_ETL = "/opt/airflow/hive-init/etl_aggregate.hql"

def run_batch_consumer(**context):
    """Run the consumer in batch mode"""
    import subprocess
    # Use bash because the script is Python
    result = subprocess.run(
        ["python", CONSUMER_SCRIPT],
        capture_output=True, text=True, env={**os.environ}
    )
    if result.returncode != 0:
        raise Exception(result.stderr)
    print(result.stdout)

def run_hive_etl(**context):
    """Execute a HiveQL aggregation query via HiveServer2Hook"""
    hook = HiveServer2Hook(hiveserver2_conn_id='hive_default')
    sql = """
        INSERT OVERWRITE TABLE weather_db.daily_summary
        SELECT 
            city,
            date_format(timestamp, 'yyyy-MM-dd') as dt,
            AVG(temp) as avg_temp,
            MAX(temp) as max_temp,
            MIN(temp) as min_temp,
            AVG(humidity) as avg_humidity,
            AVG(wind) as avg_wind
        FROM weather_db.weather_raw
        WHERE timestamp IS NOT NULL
        GROUP BY city, date_format(timestamp, 'yyyy-MM-dd');
    """
    hook.run(sql)

with DAG(
    'weather_pipeline',
    default_args=default_args,
    description='End‑to‑end weather analytics pipeline',
    schedule_interval='@hourly',
    catchup=False,
    tags=['weather'],
) as dag:

    # Task 1: fetch new weather data (re‑run producer for one cycle)
    fetch_weather = BashOperator(
        task_id='fetch_weather',
        bash_command=f'python {PRODUCER_SCRIPT}',
        env={
            'EVENTHUB_CONNECTION_STRING': os.getenv('EVENTHUB_CONNECTION_STRING'),
            'EVENTHUB_NAME': os.getenv('EVENTHUB_NAME', 'weather-raw'),
        },
    )

    # Task 2: consume from Event Hub → CSV
    ingest_to_hive = PythonOperator(
        task_id='ingest_to_hive',
        python_callable=run_batch_consumer,
    )

    # Task 3: run Hive ETL
    run_etl = PythonOperator(
        task_id='run_hive_etl',
        python_callable=run_hive_etl,
    )

    fetch_weather >> ingest_to_hive >> run_etl