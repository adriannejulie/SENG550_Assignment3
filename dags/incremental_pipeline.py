from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}


dag = DAG(
    dag_id="schedule_interval",
    default_args=default_args,
    schedule="*/4 * * * * *",   # every 4 seconds
    catchup=False,
)

run_incremental_job = BashOperator(
    task_id="run_incremental_spark",
    bash_command="python ../processing/incremental/incremental_processing.py",
    dag=dag,
)
