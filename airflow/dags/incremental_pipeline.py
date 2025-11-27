from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil
import csv
import time

# -------------------------
# Config
# -------------------------
base_dir = "/full/path/to/your/project"
raw_base = os.path.join(base_dir, "data/incremental/raw/")
spark_script_path = os.path.join(base_dir, "processing/incremental/aggregate_incremental.py")
iterations = 10  # total runs of the loop (for testing); adjust as needed
sleep_seconds = 4  # interval between runs

# -------------------------
# Default args
# -------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

# -------------------------
# DAG
# -------------------------
dag = DAG(
    dag_id='incremental_aggregation_4s',
    default_args=default_args,
    description='Incremental Spark aggregation DAG with 4s loop simulation',
    schedule=None,  # run manually or triggered externally
    start_date=datetime(2025, 11, 26),
    catchup=False,
)

# -------------------------
# Function to simulate new data arrival
# -------------------------
def simulate_new_data():
    existing_folders = [int(f) for f in os.listdir(raw_base) if f.isdigit()]
    next_folder = max(existing_folders) + 1 if existing_folders else 0
    folder_path = os.path.join(raw_base, str(next_folder))
    os.makedirs(folder_path, exist_ok=True)

    csv_file = os.path.join(folder_path, f"orders_{next_folder}.csv")
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_dow", "order_hour_of_day", "product_A", "product_B"])
        writer.writerow([0, 10, 5, 3])
        writer.writerow([0, 11, 2, 7])

    print(f"Simulated new data in folder: {folder_path}")

# -------------------------
# Function to run Spark script
# -------------------------
def run_spark_script():
    os.system(f"python3 {spark_script_path}")

# -------------------------
# Function that loops every 4s
# -------------------------
def incremental_loop():
    for i in range(iterations):
        print(f"Iteration {i+1}/{iterations}")
        simulate_new_data()  # simulate new data arrival
        run_spark_script()   # process new data
        time.sleep(sleep_seconds)

# -------------------------
# DAG Task
# -------------------------
run_incremental = PythonOperator(
    task_id="run_incremental_loop",
    python_callable=incremental_loop,
    dag=dag,
)
