# dags/data_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess
import pandas as pd
from pathlib import Path

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

def generate_data(**context):
    """Generate data and upload to MinIO"""
    cmd = [
        "python3", "-m", "src.generator.generate_data",
        "--num", "1000", "--upload", "--quiet"
    ]
    env = os.environ.copy()
    env.update({
        "MINIO_ENDPOINT": "minio:9000",
        "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER", "minioadmin"),
        "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        "MINIO_BUCKET": "incoming",
    })
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Generate failed: {result.stderr}")
        raise Exception("Data generation failed")
    print(f"Generated data: {result.stdout}")
    return result.stdout

def validate_data(**context):
    """Validate latest CSV file"""
    incoming_dir = Path("/opt/airflow/data/incoming")
    csv_files = list(incoming_dir.glob("*.csv"))
    if not csv_files:
        raise Exception("No CSV files found in incoming directory")
    
    latest_file = max(csv_files, key=os.path.getctime)
    print(f"Validating: {latest_file}")
    
    from src.processor.validate import validate_orders
    is_valid, message = validate_orders(str(latest_file))
    
    if not is_valid:
        raise Exception(f"Validation failed: {message}")
    
    print(f"Validation passed: {message}")
    return str(latest_file)

def transform_data(**context):
    """Transform validated data"""
    incoming_dir = Path("/opt/airflow/data/incoming")
    csv_files = list(incoming_dir.glob("*.csv"))
    latest_file = max(csv_files, key=os.path.getctime)
    
    output_path = Path("/opt/airflow/data/processed/orders_transformed.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    from src.processor.transform import transform_orders
    transform_orders(input_csv=str(latest_file), output_csv=str(output_path))
    
    print(f"Transformed data saved to: {output_path}")
    return str(output_path)

def upsert_data(**context):
    """Upsert transformed data to Postgres"""
    processed_file = Path("/opt/airflow/data/processed/orders_transformed.csv")
    if not processed_file.exists():
        raise Exception("Processed file not found")
    
    from src.db.upsert import upsert_orders
    upsert_orders(str(processed_file))
    
    print("Upsert completed successfully")

with DAG(
    "data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
) as dag:

    generate_task = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data,
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    upsert_task = PythonOperator(
        task_id="upsert_data",
        python_callable=upsert_data,
    )

    generate_task >> validate_task >> transform_task >> upsert_task