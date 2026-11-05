''' Basic ETL DAG '''
from datetime import datetime, date
import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(
    dag_id='basic_etl_dag',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    # 1. EXTRACT
    extract_task = BashOperator(
        task_id='extract_task',
        # Note: Updated to the working GitHub URL based on the video's warning
        bash_command='wget -c https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/top-level-domain-names.csv -O /workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-extract-data.csv'
    )

    # 2. TRANSFORM
    def transform_data():
        """Read in the file, and write a transformed file out"""
        today = date.today()
        df = pd.read_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-extract-data.csv')
        
        generic_type_df = df[df['Type'] == 'generic']
        generic_type_df['Date'] = today.strftime('%Y-%m-%d')
        
        generic_type_df.to_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-transform-data.csv', index=False)

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        dag=dag
    )

    # 3. LOAD (Cut off in the image, but this is what it looks like based on the previous lesson)
    load_task = BashOperator(
        task_id='load_task',
        bash_command='echo -e ".separator ","\n.import --skip 1 /workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-transform-data.csv top_level_domains" | sqlite3 /workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-load-db.db',
        dag=dag
    )

    # 4. SET DEPENDENCIES
    extract_task >> transform_task >> load_task