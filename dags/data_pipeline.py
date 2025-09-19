from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}



from generator.generate_data import generate_orders_csv
from processor.validate import validate_orders
from processor.transform import transform_orders
from db.upsert import upsert_orders


def generate():
    generate_orders_csv()

def validate():
    valid, err = validate_orders('data/incoming/orders.csv')
    if not valid:
        raise ValueError(f"Validation failed: {err}")

def transform():
    transform_orders()

def upsert():
    upsert_orders('data/processed/orders_transformed.csv')


with DAG('data_pipeline',
         default_args=default_args,
         schedule='@daily',
         max_active_runs=1,
         catchup=False) as dag:

    generate_task = PythonOperator(
        task_id='generate_data',
        python_callable=generate
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform
    )

    upsert_task = PythonOperator(
        task_id='upsert_data',
        python_callable=upsert
    )

    generate_task >> validate_task >> transform_task >> upsert_task
