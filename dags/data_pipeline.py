# dags/data_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import os
import sys

# allow imports from /opt/airflow/src
sys.path.insert(0, "/opt/airflow/src")

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
    # WAIT FOR DATA (present in DAG, no external action)
    # --------------------
    # This is a real task node representing "wait for files in MinIO".
    # It's implemented as a DummyOperator so it DOES NOT perform IO — it only
    # appears in the graph as the wait step you requested.
    wait_for_minio = DummyOperator(task_id="wait_for_minio")

    # --------------------
    # Generate Data Task (actual)
    # --------------------
    def run_generator(**kwargs):
        from generator.generate_data import run  # expects your generator module
        # NOTE: upload flag follows your generator API; keep as you need.
        run(num=1000, upload=False, call_upsert_flag=True)
        print("Generator ran OK")

    generate_task = PythonOperator(
        task_id="generate_data",
        python_callable=run_generator,
    )

    # --------------------
    # Validation (callable used by BranchPythonOperator)
    # --------------------
    def validation_branch_callable(**kwargs):
        """
        Run validation and branch:
         - if ok -> return 'transform_data'
         - if not ok -> return 'end_pipeline'
        This avoids raising exceptions that break the DAG; instead we cleanly end.
        """
        from processor.validate import validate_orders

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
            # No files found: treat as validation failure and end gracefully
            print("No CSV files found to validate -> ending run gracefully")
            return "end_pipeline"

        latest_file = files[0]
        ok, msg = validate_orders(latest_file)
        print(f"Validation result: {ok}, {msg}")
        if ok:
            return "transform_data"
        else:
            # Validation failed: branch to end_pipeline so pipeline doesn't 'break'
            return "end_pipeline"

    validation_branch = BranchPythonOperator(
        task_id="validation_branch",
        python_callable=validation_branch_callable,
        provide_context=True,
    )

    # End / safe exit when validation fails
    end_pipeline = DummyOperator(task_id="end_pipeline")

    # --------------------
    # Transform Data Task (actual)
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
    # Upsert Data Task (actual)
    # --------------------
    def run_upsert(**kwargs):
        src_path = "/opt/airflow"
        if src_path not in sys.path:
            sys.path.insert(0, src_path)

        try:
            from src.db.upsert import upsert_orders
        except ModuleNotFoundError as e:
            print("Cannot import upsert_orders:", e)
            raise

        csv_path = "/opt/airflow/data/processed/orders_transformed.csv"
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{csv_path} not found")

        upsert_orders(csv_path)
        print(f"Upsert completed at {datetime.utcnow()}")

    upsert_task = PythonOperator(
        task_id="upsert_data",
        python_callable=run_upsert,
    )

    # --------------------
    # Archive File (present in DAG, no external action)
    # --------------------
    # Real name and node, but implemented as DummyOperator so it doesn't do IO.
    archive_file = DummyOperator(task_id="archive_file")

    # --------------------
    # Dependencies
    # wait -> generate -> validation_branch -> (transform OR end) -> upsert -> archive
    # --------------------
    wait_for_minio >> generate_task >> validation_branch
    validation_branch >> transform_task >> upsert_task >> archive_file
    validation_branch >> end_pipeline
