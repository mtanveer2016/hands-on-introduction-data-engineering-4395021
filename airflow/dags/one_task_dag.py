from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='one_task_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Trigger manually only, does not run on a schedule
    catchup=False,
    tags=['example'],
) as dag:

    task1 = BashOperator(
        task_id='one_task',
        # Added the full workspace path to the command
        bash_command='echo "hello, linkedin learning" > /workspaces/hands-on-introduction-data-engineering-4395021/create-this-file.txt',
        dag=dag
    )