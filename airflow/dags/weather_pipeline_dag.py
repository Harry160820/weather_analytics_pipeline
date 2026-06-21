import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

EH_CONN_STR = os.getenv("EVENTHUB_CONNECTION_STRING", "")
PG_HOST = os.getenv("POSTGRES_HOST", "")
PG_USER = os.getenv("POSTGRES_USER", "")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
PG_DB = os.getenv("POSTGRES_DB", "")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_pipeline',
    default_args=default_args,
    description='Weather analytics pipeline',
    schedule_interval='@hourly',
    catchup=False,
    tags=['weather'],
) as dag:

    fetch_weather = BashOperator(
        task_id='fetch_weather',
        bash_command='python /opt/airflow/producer/weather_producer_once.py',
        env={
            'EVENTHUB_CONNECTION_STRING': EH_CONN_STR,
            'EVENTHUB_NAME': 'weather-raw',
        },
    )

    ingest_to_csv = BashOperator(
        task_id='ingest_to_csv',
        bash_command='python /opt/airflow/consumer/consume_batch.py',
        env={
            'EVENTHUB_CONNECTION_STRING': EH_CONN_STR,
            'EVENTHUB_NAME': 'weather-raw',
        },
    )

    # Hive ETL: run beeline inside the hive container (always works)
    run_etl = BashOperator(
        task_id='run_hive_etl',
        bash_command="""
            docker exec hive-standalone /opt/hive/bin/beeline \
                -u jdbc:hive2://localhost:10000/default \
                -e "
                    CREATE TABLE IF NOT EXISTS weather_db.daily_summary (
                        city STRING, dt STRING, avg_temp DOUBLE, max_temp DOUBLE,
                        min_temp DOUBLE, avg_humidity DOUBLE, avg_wind DOUBLE
                    ) STORED AS TEXTFILE LOCATION '/data/daily_summary';

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
                "
        """,
    )

    # Load to PostgreSQL: export Hive table to CSV, then import via psql
    load_pg = BashOperator(
        task_id='load_to_postgres',
        bash_command=f"""
            # Export daily_summary to a temporary CSV on the host
            docker exec hive-standalone /opt/hive/bin/beeline \
                --outputformat=csv2 \
                -u jdbc:hive2://localhost:10000/default \
                -e "SELECT city, dt, avg_temp, max_temp, min_temp, avg_humidity, avg_wind FROM weather_db.daily_summary" \
                > /tmp/daily_summary_export.csv

            # Load into PostgreSQL (requires psql client in Airflow container)
            PGPASSWORD="{PG_PASSWORD}" psql -h {PG_HOST} -U {PG_USER} -d {PG_DB} -c "
                CREATE TABLE IF NOT EXISTS daily_summary (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(100),
                    dt DATE,
                    avg_temp DECIMAL(5,2),
                    max_temp DECIMAL(5,2),
                    min_temp DECIMAL(5,2),
                    avg_humidity DECIMAL(5,2),
                    avg_wind DECIMAL(5,2),
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(city, dt)
                );
            "

            # Import CSV into PostgreSQL
            PGPASSWORD="{PG_PASSWORD}" psql -h {PG_HOST} -U {PG_USER} -d {PG_DB} -c "
                \\COPY daily_summary (city, dt, avg_temp, max_temp, min_temp, avg_humidity, avg_wind)
                FROM '/tmp/daily_summary_export.csv' DELIMITER ',' CSV HEADER
                ON CONFLICT (city, dt) DO UPDATE SET
                    avg_temp = EXCLUDED.avg_temp,
                    max_temp = EXCLUDED.max_temp,
                    min_temp = EXCLUDED.min_temp,
                    avg_humidity = EXCLUDED.avg_humidity,
                    avg_wind = EXCLUDED.avg_wind;
            "
        """,
    )

    fetch_weather >> ingest_to_csv >> run_etl >> load_pg
