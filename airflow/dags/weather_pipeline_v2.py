import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2
import subprocess, csv, io

EH_CONN_STR = os.getenv("EVENTHUB_CONNECTION_STRING", "")
PG_HOST = os.getenv("POSTGRES_HOST", "")
PG_USER = os.getenv("POSTGRES_USER", "")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
PG_DB = os.getenv("POSTGRES_DB", "")

def load_to_postgres(**context):
    result = subprocess.run(
        ["docker", "exec", "hive-standalone", "/opt/hive/bin/beeline",
         "--outputformat=csv2", "-u", "jdbc:hive2://localhost:10000/default",
         "-e", "SELECT city, dt, avg_temp, max_temp, min_temp, avg_humidity, avg_wind FROM weather_db.daily_summary"],
        capture_output=True, text=True
    )
    if result.returncode != 0 or not result.stdout.strip():
        print("No data from Hive")
        return

    reader = csv.DictReader(io.StringIO(result.stdout), delimiter=',')
    rows = []
    for row in reader:
        rows.append((row['city'], row['dt'], float(row['avg_temp']), float(row['max_temp']),
                     float(row['min_temp']), float(row['avg_humidity']), float(row['avg_wind'])))
    if not rows:
        print("No rows")
        return

    conn_pg = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = conn_pg.cursor()
    for r in rows:
        cur.execute("""
            INSERT INTO daily_summary (city, dt, avg_temp, max_temp, min_temp, avg_humidity, avg_wind)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city, dt) DO UPDATE SET
                avg_temp = EXCLUDED.avg_temp,
                max_temp = EXCLUDED.max_temp,
                min_temp = EXCLUDED.min_temp,
                avg_humidity = EXCLUDED.avg_humidity,
                avg_wind = EXCLUDED.avg_wind
        """, r)
    conn_pg.commit()
    cur.close()
    conn_pg.close()
    print(f"Loaded {len(rows)} rows to PostgreSQL.")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_pipeline_v2',
    default_args=default_args,
    description='Weather analytics pipeline v2',
    schedule_interval='@hourly',
    catchup=False,
    tags=['weather'],
) as dag:

    fetch_weather = BashOperator(
        task_id='fetch_weather',
        bash_command='python /opt/airflow/producer/weather_producer_once.py',
        env={'EVENTHUB_CONNECTION_STRING': EH_CONN_STR, 'EVENTHUB_NAME': 'weather-raw'},
    )

    ingest_to_csv = BashOperator(
        task_id='ingest_to_csv',
        bash_command='python /opt/airflow/consumer/consume_batch.py',
        env={'EVENTHUB_CONNECTION_STRING': EH_CONN_STR, 'EVENTHUB_NAME': 'weather-raw'},
    )

        run_etl = BashOperator(
        task_id='run_hive_etl',
        bash_command="""
            docker exec hive-standalone /opt/hive/bin/beeline \
                -u jdbc:hive2://localhost:10000/default \
                -e "
                    USE weather_db;

                    CREATE EXTERNAL TABLE IF NOT EXISTS weather_raw (
                        city STRING,
                        \`timestamp\` STRING,
                        temp DOUBLE,
                        feels_like DOUBLE,
                        humidity INT,
                        pressure DOUBLE,
                        weather STRING,
                        \`desc\` STRING,
                        wind DOUBLE,
                        clouds INT
                    )
                    ROW FORMAT DELIMITED
                    FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '/data/weather';

                    CREATE TABLE IF NOT EXISTS daily_summary (
                        city STRING,
                        dt STRING,
                        avg_temp DOUBLE,
                        max_temp DOUBLE,
                        min_temp DOUBLE,
                        avg_humidity DOUBLE,
                        avg_wind DOUBLE
                    )
                    STORED AS TEXTFILE
                    LOCATION '/data/daily_summary';

                    INSERT OVERWRITE TABLE daily_summary
                    SELECT
                        city,
                        date_format(\`timestamp\`, 'yyyy-MM-dd') as dt,
                        AVG(temp), MAX(temp), MIN(temp),
                        AVG(humidity), AVG(wind)
                    FROM weather_raw
                    WHERE \`timestamp\` IS NOT NULL
                    GROUP BY city, date_format(\`timestamp\`, 'yyyy-MM-dd');
                "
        """,
    )

    load_pg = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    fetch_weather >> ingest_to_csv >> run_etl >> load_pg