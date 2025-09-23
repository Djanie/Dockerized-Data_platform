# dags/data_pipeline.py
"""Airflow DAG for orchestrating a data pipeline including generation, validation, transformation, and upsert operations."""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Allow imports from /opt/airflow/src
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

    # Task to represent waiting for data in MinIO
    wait_for_minio = sensorOperator(task_id="wait_for_minio")

    # Task to generate data
    def run_generator(**kwargs):
        """Execute data generation and upload process."""
        try:
            from generator.generate_data import run
            run(num=1000, upload=False, call_upsert_flag=True)
            logger.info("Generator ran successfully")
        except Exception as e:
            logger.error("Generator execution failed: %s", e)
            raise

    generate_task = PythonOperator(
        task_id="generate_data",
        python_callable=run_generator,
    )

    # Task to branch based on validation result
    def validation_branch_callable(**kwargs):
        """Determine the next task based on validation result."""
        try:
            from processor.validate import validate_orders

            incoming_dir = "/opt/airflow/data/incoming"
            files = sorted(
                [os.path.join(incoming_dir, f) for f in os.listdir(incoming_dir) if f.endswith(".csv")],
                key=os.path.getmtime,
                reverse=True,
            )
            if not files:
                logger.warning("No CSV files found to validate")
                return "end_pipeline"

            latest_file = files[0]
            ok, msg = validate_orders(latest_file)
            logger.info("Validation result: %s, %s", ok, msg)
            return "transform_data" if ok else "end_pipeline"
        except Exception as e:
            logger.error("Validation branching failed: %s", e)
            raise

    validation_branch = BranchPythonOperator(
        task_id="validation_branch",
        python_callable=validation_branch_callable,
        provide_context=True,
    )

    # Task to end pipeline gracefully on validation failure
    end_pipeline = DummyOperator(task_id="end_pipeline")

    # Task to transform data
    def run_transform(**kwargs):
        """Transform the validated data into processed format."""
        try:
            from processor.transform import transform_orders
            transform_orders()
            logger.info("Transform completed successfully")
        except Exception as e:
            logger.error("Transform execution failed: %s", e)
            raise

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=run_transform,
    )

    # Task to upsert data into PostgreSQL
    def run_upsert(**kwargs):
        """Upsert transformed data into the PostgreSQL database."""
        try:
            src_path = "/opt/airflow"
            if src_path not in sys.path:
                sys.path.insert(0, src_path)

            from src.db.upsert import upsert_orders

            csv_path = "/opt/airflow/data/processed/orders_transformed.csv"
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"{csv_path} not found")

            upsert_orders(csv_path)
            logger.info("Upsert completed at %s", datetime.utcnow())
        except ModuleNotFoundError as e:
            logger.error("Module import failed: %s", e)
            raise
        except FileNotFoundError as e:
            logger.error("File not found: %s", e)
            raise
        except Exception as e:
            logger.error("Upsert execution failed: %s", e)
            raise

    upsert_task = PythonOperator(
        task_id="upsert_data",
        python_callable=run_upsert,
    )

    # Task to represent archiving of processed files
    archive_file = DummyOperator(task_id="archive_file")

    # Define task dependencies
    wait_for_minio >> generate_task >> validation_branch
    validation_branch >> transform_task >> upsert_task >> archive_file
    validation_branch >> end_pipeline