from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys, os, traceback

# Ensure Python can import the "src" package (parent of src must be on sys.path)
ROOT = "/opt/airflow"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

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

def generate_data(**kwargs):
    """
    Generate sample CSV files into /opt/airflow/data/incoming.
    Adjust call to your generator function signature if needed.
    """
    try:
        # debug
        print("DEBUG generate_data sys.path[:8]:", sys.path[:8])
        print("DEBUG generate_data PYTHONPATH:", os.environ.get("PYTHONPATH"))

        # lazy import to avoid parse-time errors
        from src.generator.generate_data import generate_orders_csv

        out_dir = "/opt/airflow/data/incoming"
        os.makedirs(out_dir, exist_ok=True)

        # call generator (change n_files if you want more)
        generate_orders_csv(out_dir, n_files=1)  # adjust arg names if your function differs
        print("Generator finished writing to", out_dir)
    except Exception:
        print("Generator error:")
        traceback.print_exc()
        raise

def validate_data(**kwargs):
    try:
        # debug
        print("DEBUG validate_data sys.path[:8]:", sys.path[:8])
        print("DEBUG validate_data PYTHONPATH:", os.environ.get("PYTHONPATH"))

        # lazy import
        from src.processor.validate import validate_orders

        incoming_dir = "/opt/airflow/data/incoming"
        for file in sorted(os.listdir(incoming_dir)):
            if file.endswith(".csv"):
                file_path = os.path.join(incoming_dir, file)
                print(f"Validating: {file_path}")
                validate_orders(file_path)
        print("Validation finished.")
    except Exception:
        print("Validation error:")
        traceback.print_exc()
        raise

with DAG(
    "data_pipeline",
    default_args=default_args,
    description="Online Orders Data Pipeline (generate -> validate)",
    schedule_interval=None,
    catchup=False,
) as dag:

    gen = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data,
    )

    val = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    gen >> val
