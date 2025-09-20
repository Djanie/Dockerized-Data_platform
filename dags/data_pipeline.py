from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys, os

# --- force /opt/airflow/src on sys.path for imports ---
SRC_PATH = "/opt/airflow/src"
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

print("DEBUG DAG LOAD: cwd:", os.getcwd())
print("DEBUG DAG LOAD: PYTHONPATH env:", os.environ.get("PYTHONPATH"))
print("DEBUG DAG LOAD: sys.path[:5]:", sys.path[:5])

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

def validate_data(**kwargs):
    import sys, os
    print("DEBUG validate_data sys.path[:5]:", sys.path[:5])
    print("DEBUG validate_data PYTHONPATH:", os.environ.get("PYTHONPATH"))

    from src.processor.validate import validate_orders

    incoming_dir = "/opt/airflow/data/incoming"
    for file in os.listdir(incoming_dir):
        if file.endswith(".csv"):
            file_path = os.path.join(incoming_dir, file)
            print(f"Validating: {file_path}")
            validate_orders(file_path)

with DAG(
    "data_pipeline",
    default_args=default_args,
    description="Online Orders Data Pipeline",
    schedule_interval=None,
    catchup=False,
) as dag:

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        provide_context=True,
    )
