from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='two_task_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example'],
) as dag:

    # Define Task T0
    t0 = BashOperator(
        task_id='Task_T0',
        bash_command='echo "This is Task T0 completing."',
    )

    # Define Task T1
    t1 = BashOperator(
        task_id='Task_T1',
        bash_command='echo "Task T1 is now running because T0 succeeded."',
    )

    # Set the dependency: T0 must finish before T1 starts
    t0 >> t1