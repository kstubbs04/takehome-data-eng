from datetime import datetime, timedelta
import time

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
        'fetcher',
        default_args=default_args,
        description='To fetch the weather data',
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['take-home'],
) as dag:

    # @TODO: Add your function here. Example here: https://airflow.apache.org/docs/apache-airflow/stable/_modules/airflow/example_dags/example_python_operator.html
    # Hint: How to fetch the weather data from https://www.aerisweather.com/?
    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(random_base)

    # Create table to store raw data if it doesnt exists
    create_staging_table = PostgresOperator(
                                postgres_conn_id="postgres",
                                task_id="create_raw_dataset",
                                sql="""
                                    CREATE TABLE IF NOT EXISTS raw_current_weather (
                                        id serial NOT NULL PRIMARY KEY,
                                        data json NOT NULL
                                   );
                                  """,
                            )

    fetch_weather_data = PythonOperator(
        task_id='ingest_api_data',
        python_callable=my_sleeping_function,
        op_kwargs={'random_base': 101.0 / 10},
    )

    # @TODO: Fill in the below
    persist_raw_json = PostgresOperator(
        task_id="store_dataset",
        sql="""
            INSERT INTO raw_current_weather
          """,
    )

    create_staging_table >> fetch_weather_data >> persist_raw_json