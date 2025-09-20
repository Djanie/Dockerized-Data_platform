# dags/data_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add src folder to PYTHONPATH inside DAG
sys.path.insert(0, "/opt/airflow/src")

# Default arguments for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

with DAG(
    "data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
) as dag:

    # --------------------
    # Generate Data Task
    # --------------------
    def run_generator(**kwargs):
        from generator.generate_data import run   # use the new run() function
        run(num=1000, upload=False, call_upsert_flag=True)
        print("Generator ran OK")

    generate_task = PythonOperator(
        task_id="generate_data",
        python_callable=run_generator,
    )

    # --------------------
    # Validate Data Task
    # --------------------
    def run_validator(**kwargs):
        from processor.validate import validate_orders

        # pick latest CSV
        incoming_dir = "/opt/airflow/data/incoming"
        files = sorted(
            [
                os.path.join(incoming_dir, f)
                for f in os.listdir(incoming_dir)
                if f.endswith(".csv")
            ],
            key=os.path.getmtime,
            reverse=True,
        )
        if not files:
            raise FileNotFoundError("No CSV files found to validate")
        latest_file = files[0]

        ok, msg = validate_orders(latest_file)
        print(f"Validation result: {ok}, {msg}")
        if not ok:
            raise ValueError(f"Validation failed: {msg}")

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=run_validator,
    )

    # --------------------
    # Transform Data Task
    # --------------------
    def run_transform(**kwargs):
        from processor.transform import transform_orders

        transform_orders()
        print("Transform done")

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=run_transform,
    )

    # --------------------
    # Upsert Data Task
    # --------------------
    def run_upsert(**kwargs):
        # Ensure Airflow can find your src package
        src_path = "/opt/airflow"
        if src_path not in sys.path:
            sys.path.insert(0, src_path)

        try:
            from src.db.upsert import upsert_orders
        except ModuleNotFoundError as e:
            print("Cannot import upsert_orders:", e)
            raise

        # Path to the transformed CSV
        csv_path = "/opt/airflow/data/processed/orders_transformed.csv"
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{csv_path} not found")

        # Call your upsert function
        upsert_orders(csv_path)
        print(f"Upsert completed at {datetime.utcnow()}")

    upsert_task = PythonOperator(
        task_id="upsert_data",
        python_callable=run_upsert,
    )

    # --------------------
    # Set Dependencies
    # --------------------
    generate_task >> validate_task >> transform_task >> upsert_task
