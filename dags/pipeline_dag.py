#from airflow.decorators import dag, task
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

def example_task():
    print("This is an example task.")

with DAG(
    dag_id="pipeline_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    
    print_task = PythonOperator(
        task_id="print_task",
        python_callable=example_task,
    )
