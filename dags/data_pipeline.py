# dags/data_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import os, sys, logging

# Make sure Python can import the repo package "src"
if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

with DAG(
    "data_pipeline_with_placeholders",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
) as dag:

    # ---------- placeholder: wait for MinIO / file arrival ----------
    wait_for_minio = DummyOperator(
        task_id="wait_for_minio_placeholder",
        doc_md="Placeholder for a sensor that waits for a file in MinIO. (Dummy — does nothing.)",
    )

    # ---------- generate ----------
    def run_generator(**kwargs):
        # use the generator's run() entrypoint (integrated)
        from src.generator.generate_data import run
        logging.info("Running generator (placeholder args).")
        # choose parameters that make sense for your tests
        run(num=500, upload=False, call_upsert_flag=False)
        logging.info("Generator completed.")

    generate_task = PythonOperator(
        task_id="generate_data",
        python_callable=run_generator,
    )

    # ---------- validate (returns True/False via XCom) ----------
    def run_validator(**kwargs):
        from src.processor.validate import validate_orders
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
            logging.warning("No CSVs found to validate")
            # return False so branch will end gracefully
            return False
        latest = files[0]
        ok, msg = validate_orders(latest)
        logging.info("Validation result: %s -- %s", ok, msg)
        return bool(ok)

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=run_validator,
    )

    # ---------- Branch depending on validation ----------
    def decide_branch(**kwargs):
        ti = kwargs["ti"]
        val = ti.xcom_pull(task_ids="validate_data")
        # val is True/False (or None)
        if val:
            return "transform_data"
        else:
            return "end_on_validation_fail"

    branch_after_validate = BranchPythonOperator(
        task_id="branch_after_validate",
        python_callable=decide_branch,
        provide_context=True,
    )

    end_on_validation_fail = DummyOperator(
        task_id="end_on_validation_fail",
        doc_md="End path when validation fails (graceful stop).",
    )

    # ---------- transform ----------
    def run_transform(**kwargs):
        from src.processor.transform import transform_orders
        # transform_orders() should read from incoming and write to /opt/airflow/data/processed/
        transform_orders()
        logging.info("Transform completed.")

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=run_transform,
    )

    # ---------- upsert ----------
    def run_upsert(**kwargs):
        # ensure parent of src is on sys.path (so `from src...` works inside upsert)
        if "/opt/airflow" not in sys.path:
            sys.path.insert(0, "/opt/airflow")

        from src.db.upsert import upsert_orders

        csv_path = "/opt/airflow/data/processed/orders_transformed.csv"
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{csv_path} not found; cannot upsert")
        upsert_orders(csv_path)
        logging.info("Upsert completed.")

    upsert_task = PythonOperator(
        task_id="upsert_data",
        python_callable=run_upsert,
    )

    # ---------- placeholder: archive file ----------
    archive_file = DummyOperator(
        task_id="archive_file_placeholder",
        doc_md="Placeholder for archiving the source CSV to archive storage (dummy).",
    )

    # ---------- final end ----------
    end_success = DummyOperator(task_id="end_success", trigger_rule="none_failed")

    # ---------- wiring ----------
    # generate -> wait placeholder -> validate -> branch -> (end_on_validation_fail OR transform -> upsert -> archive -> end_success)
    generate_task >> wait_for_minio >> validate_task >> branch_after_validate
    branch_after_validate >> end_on_validation_fail
    branch_after_validate >> transform_task
    transform_task >> upsert_task >> archive_file >> end_success
