from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pendulum
import sys
import os
import logging
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from pipeline import get_all_workouts, insert_sql
from utils import clean_workouts, get_row_count, save_table_to_csv, send_email

# Important commands
# When adding/changing dag files
# -- airflow db reset / airflow db migrate
# -- airflow dags reserialize
# make sure they're unpaused -- won't show up in airflow dags list if paused
# -- airflow dags unpause <dag_name>
# testing:
# -- airflow dags test <dag_name> to test without scheduler
# DAG
# airflow

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s -- %(levelname)s -- %(message)s'
)

local_tz = pendulum.timezone("America/New_York")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 12, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='workout_pipeline',
    default_args=default_args,
    schedule=timedelta(days=1),
    catchup=False,
)
def run_pipeline():
    # get the workouts
    @task
    def get_all_workouts_task():
        workouts = get_all_workouts()
        return workouts
    # clean the workouts
    @task
    def cleaned_workouts_task(workouts=None):
        cleaned_workouts = clean_workouts(workouts)
        return cleaned_workouts
    # get the number of rows
    @task
    def get_row_count_task():
        hook = PostgresHook(postgres_conn_id='workouts_conn')
        conn = hook.get_conn()
        row_count = get_row_count(conn)
        conn.close()
        return row_count
    # insert the data into the DB
    @task
    def insert_sql_task(cleaned_workouts=None):
        hook = PostgresHook(postgres_conn_id='workouts_conn')
        conn = hook.get_conn()
        insert_sql(conn, cleaned_workouts)
        conn.close()
    # calculate the number of rows inserted
    @task
    def calculate_row_diff_task(initial_count=None, final_count=None):
        return final_count - initial_count
    
    workouts = get_all_workouts_task()
    cleaned = cleaned_workouts_task(workouts)

    initial_count = get_row_count_task.override(task_id='initial_row_count')()
    insert_sql_task(cleaned)
    final_count = get_row_count_task.override(task_id='final_row_count')()

    row_diff = calculate_row_diff_task(initial_count, final_count)

    initial_count >> final_count >> row_diff
    workouts >> cleaned

dag = run_pipeline()