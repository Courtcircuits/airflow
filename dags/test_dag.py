from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

# Default arguments to apply to all tasks
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 10, 15),  # Replace with your start date
}

# Function for PythonOperator


def task_1_function(**kwargs):
    print("Task 1 is executing")


def task_2_function(**kwargs):
    print("Task 2 is executing")


# Define the DAG
with DAG(
    'example_dag',  # DAG name
    default_args=default_args,
    description='A simple DAG example',
    schedule_interval=timedelta(days=1),  # Run once a day
    catchup=False  # Skip running previous dates if the DAG start date is in the past
) as dag:

    # Define tasks
    start = DummyOperator(task_id='start')

    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=task_1_function,
        provide_context=True
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=task_2_function,
        provide_context=True
    )

    end = DummyOperator(task_id='end')

    # Set task dependencies
    start >> task_1 >> task_2 >> end
