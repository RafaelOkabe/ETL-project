from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='aetl_medallion',
    start_date=datetime(2025, 5, 31),
    schedule_interval='@daily',
    catchup=False
) as dag:

    bronze = BashOperator(
        task_id='bronze_task',
        bash_command='python /opt/airflow/dags/scripts/bronze.py'
    )

    silver = BashOperator(
        task_id='silver_task',
        bash_command='python /opt/airflow/dags/scripts/silver.py'
    )

    gold = BashOperator(
        task_id='gold_task',
        bash_command='python /opt/airflow/dags/scripts/gold.py'
    )

    bronze >> silver >> gold
