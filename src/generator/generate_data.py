# src/generator/generate_data.py
"""Generator integrated with validator, MinIO upload, and file_registry + optional upsert call.

Usage:
  # generate 500 rows, upload to MinIO, call upsert
  python -m src.generator.generate_data --num 500 --upload --call-upsert

Environment (used if CLI args not provided):
  MINIO_ENDPOINT (e.g. localhost:9000)
  MINIO_ROOT_USER / MINIO_ROOT_PASSWORD
  MINIO_BUCKET (defaults to 'incoming')
  POSTGRES_* envs used by src.db.connection.get_db_engine()

This file deliberately uses lazy imports for networked libs.
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

import pandas as pd
from faker import Faker

# Lazy optional imports placeholders
Minio = None
S3Error = Exception

try:
    # don't raise if missing; we import inside functions when needed
    from minio import Minio as _Minio
    from minio.error import S3Error as _S3Error
    Minio = _Minio
    S3Error = _S3Error
except Exception:
    Minio = None
    S3Error = Exception

# NOTE: we will import SQLAlchemy engine via your connection function lazily when needed
# import of your validator and upsert will also be lazy to avoid network calls at import time

# ---------------- Config ----------------
@dataclass
class CFG:
    out_dir: Path = Path("data/incoming")
    prefix: str = "orders"
    num: int = 1000
    seed: Optional[int] = None
    duplicate_frac: float = 0.05
    null_rating_frac: float = 0.02
    write_parquet: bool = False

    # MinIO
    minio_endpoint: Optional[str] = os.getenv("MINIO_ENDPOINT", None)
    minio_access: Optional[str] = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY"))
    minio_secret: Optional[str] = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY"))
    minio_bucket: str = os.getenv("MINIO_BUCKET", "incoming")
    minio_secure: bool = bool(os.getenv("MINIO_SECURE", False))
    minio_retries: int = 3
    minio_backoff: int = 2

    # Postgres registration (uses your connection.get_db_engine)
    pg_retries: int = 3
    pg_backoff: int = 2

    upload: bool = False
    call_upsert: bool = False
    quiet: bool = False
    fail_on_upload_fail: bool = False

# ---------------- Logging ----------------
def setup_logging(quiet: bool):
    level = logging.WARNING if quiet else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s: %(message)s")

# ---------------- Data generation (keeps your original logic) ----------------
def generate_df(cfg: CFG) -> pd.DataFrame:
    fake = Faker()
    if cfg.seed is not None:
        Faker.seed(cfg.seed)
    import random

    rows = []
    cuisines = ['Italian', 'Chinese', 'Mexican', 'Indian', 'American']
    for _ in range(cfg.num):
        order_time = fake.date_time_this_year()
        delivery_time = fake.date_time_between(start_date=order_time)
        delivery_duration = (delivery_time - order_time).total_seconds() / 60.0
        record = {
            'order_id': fake.uuid4(),
            'restaurant_id': random.randint(1, 100),
            'customer_id': random.randint(1, 500),
            'cuisine': random.choice(cuisines),
            'order_time': order_time,
            'delivery_time': delivery_time,
            'delivery_duration_minutes': round(delivery_duration, 2),
            'distance_km': round(random.uniform(0.1, 20.0), 2),
            'rating': random.choice([1, 2, 3, 4, 5, None]),
            'amount': round(random.uniform(10, 200), 2)
        }
        # random nulls (approx)
        if random.random() < cfg.null_rating_frac:
            record['rating'] = None
        rows.append(record)

    df = pd.DataFrame(rows)

    # Inject duplicates (~duplicate_frac)
    if 0.0 < cfg.duplicate_frac < 1.0:
        dup = df.sample(frac=cfg.duplicate_frac, replace=False, random_state=cfg.seed or 1)
        df = pd.concat([df, dup], ignore_index=True)

    logging.info("Generated %d records (including duplicates)", len(df))
    return df

# ---------------- File writing ----------------
def _timestamped_path(out_dir: Path, prefix: str, ext: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = f"{prefix}_{ts}"
    p = out_dir / f"{base}.{ext}"
    i = 1
    while p.exists():
        p = out_dir / f"{base}_{i}.{ext}"
        i += 1
    return p

def write_csv(cfg: CFG, df: pd.DataFrame) -> Path:
    # convert datetimes to ISO strings for CSV portability (so your validator's parse_dates works)
    df_copy = df.copy()
    for c in df_copy.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df_copy[c] = df_copy[c].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    p = _timestamped_path(cfg.out_dir, cfg.prefix, "csv")
    df_copy.to_csv(p, index=False)
    logging.info("Wrote CSV to %s", p)
    return p

# ---------------- MinIO upload (lazy safe) ----------------
class _MinioLazy:
    def __init__(self, cfg: CFG):
        self.cfg = cfg
        self._client = None

    def _client_get(self):
        if self._client is None:
            if Minio is None:
                raise RuntimeError("minio package not installed")
            # Minio endpoint might be like 'localhost:9000' — MinIO SDK accepts that.
            self._client = Minio(self.cfg.minio_endpoint, access_key=self.cfg.minio_access, secret_key=self.cfg.minio_secret, secure=self.cfg.minio_secure)
        return self._client

    def ensure_bucket(self, bucket: str):
        c = self._client_get()
        if not c.bucket_exists(bucket):
            logging.info("Creating MinIO bucket %s", bucket)
            c.make_bucket(bucket)

    def upload_file(self, local_path: Path, object_name: Optional[str] = None):
        client = self._client_get()
        obj = object_name or local_path.name
        client.fput_object(self.cfg.minio_bucket, obj, str(local_path))
        logging.info("Uploaded file to MinIO as %s/%s", self.cfg.minio_bucket, obj)
        return obj

def upload_with_retries(cfg: CFG, local_path: Path) -> Tuple[bool, Optional[str]]:
    if not (cfg.minio_endpoint and cfg.minio_access and cfg.minio_secret):
        logging.warning("MinIO credentials/config incomplete; skipping upload")
        return False, None
    helper = _MinioLazy(cfg)
    last_exc = None
    for attempt in range(1, cfg.minio_retries + 1):
        try:
            helper.ensure_bucket(cfg.minio_bucket)
            obj = helper.upload_file(local_path)
            return True, obj
        except (S3Error, Exception) as e:
            logging.warning("MinIO upload attempt %d/%d failed: %s", attempt, cfg.minio_retries, e)
            last_exc = e
            time.sleep(cfg.minio_backoff ** (attempt - 1))
    logging.error("MinIO upload failed after %d attempts: %s", cfg.minio_retries, last_exc)
    return False, None

# ---------------- file_registry using your db connection (lazy SQLAlchemy) ----------------
def _get_db_engine():
    # Lazy import of your project's connection function
    try:
        from src.db.connection import get_db_engine
    except Exception as e:
        logging.warning("Could not import src.db.connection.get_db_engine: %s", e)
        return None
    try:
        engine = get_db_engine()
    except Exception as e:
        logging.warning("get_db_engine() failed: %s", e)
        return None
    return engine

def ensure_file_registry(engine) -> None:
    # create table if not exists using SQL
    ddl = """
    CREATE TABLE IF NOT EXISTS file_registry (
        id SERIAL PRIMARY KEY,
        file_name TEXT NOT NULL,
        uploaded_at TIMESTAMPTZ NOT NULL,
        processed_at TIMESTAMPTZ,
        status TEXT NOT NULL,
        rows_in INTEGER,
        rows_out INTEGER,
        duplicate_count INTEGER,
        validation_summary JSONB
    );
    """
    with engine.connect() as conn:
        conn.execute(ddl)

def insert_file_record(engine, file_name: str, uploaded_at: datetime, status: str, rows_in: int, duplicate_count: int, validation_summary: Dict[str, Any]) -> Optional[int]:
    insert_sql = """
    INSERT INTO file_registry (file_name, uploaded_at, status, rows_in, duplicate_count, validation_summary)
    VALUES (:file_name, :uploaded_at, :status, :rows_in, :duplicate_count, :validation_summary)
    RETURNING id;
    """
    with engine.connect() as conn:
        result = conn.execute(
            insert_sql,
            {
                "file_name": file_name,
                "uploaded_at": uploaded_at,
                "status": status,
                "rows_in": rows_in,
                "duplicate_count": duplicate_count,
                "validation_summary": json.dumps(validation_summary),
            },
        )
        row = result.fetchone()
        if row:
            record_id = int(row[0])
            logging.info("Inserted file_registry id=%s", record_id)
            return record_id
    return None

def update_file_record_processed(engine, record_id: int, processed_at: datetime, status: str, rows_out: Optional[int] = None, validation_summary: Optional[Dict[str, Any]] = None):
    update_sql = """
    UPDATE file_registry
    SET processed_at = :processed_at,
        status = :status,
        rows_out = COALESCE(:rows_out, rows_out),
        validation_summary = COALESCE(:validation_summary, validation_summary)
    WHERE id = :id;
    """
    with engine.connect() as conn:
        conn.execute(
            update_sql,
            {
                "processed_at": processed_at,
                "status": status,
                "rows_out": rows_out,
                "validation_summary": json.dumps(validation_summary) if validation_summary is not None else None,
                "id": record_id,
            },
        )
    logging.info("Updated file_registry id=%s -> %s", record_id, status)

# ---------------- Validation summary helper ----------------
def make_validation_summary_from_df(df: pd.DataFrame) -> Dict[str, Any]:
    total = len(df)
    dup_count = int(df.duplicated(keep=False).sum())
    nulls = {c: int(df[c].isnull().sum()) for c in df.columns}
    schema = {c: str(dtype) for c, dtype in df.dtypes.items()}
    return {"rows_in": total, "duplicate_count": dup_count, "null_counts": nulls, "schema": schema}

# ---------------- Call your upsert (lazy) ----------------
def call_project_upsert(local_file: Path) -> Any:
    try:
        # try the path you provided: src.db.upsert.upsert_orders
        from src.db.upsert import upsert_orders
    except Exception as e:
        logging.warning("Could not import src.db.upsert.upsert_orders: %s", e)
        return None
    try:
        # upsert_orders expects a csv path; your function handles table creation and upsert
        logging.info("Calling upsert_orders(%s)", local_file)
        result = upsert_orders(str(local_file))
        return result
    except Exception as e:
        logging.exception("upsert_orders raised: %s", e)
        raise

# ---------------- Integrate your validator (lazy) ----------------
def call_validator(csv_path: str) -> Tuple[bool, Optional[str]]:
    try:
        from src.processor.validate import validate_orders
    except Exception as e:
        logging.warning("Could not import src.processor.validate.validate_orders: %s", e)
        return False, f"validator not available: {e}"
    try:
        return validate_orders(csv_path)
    except Exception as e:
        logging.exception("Validator raised: %s", e)
        return False, str(e)

# ---------------- Main run flow ----------------
def run(
    *,
    num: int = 1000,
    upload: bool = False,
    call_upsert_flag: bool = False,
    write_parquet: bool = False,
    quiet: bool = False,
) -> int:
    cfg = CFG(out_dir=Path("data/incoming"), prefix="orders", num=num, write_parquet=write_parquet, upload=upload, call_upsert=call_upsert_flag, quiet=quiet)
    setup_logging(cfg.quiet)

    # 1) generate
    df = generate_df(cfg)

    # 2) write local CSV (timestamped) — keep datetimes as ISO strings so validator can parse
    out_path = write_csv(cfg, df)

    # 3) run your validator (uses parse_dates inside your validator)
    is_valid, validation_msg = call_validator(str(out_path))

    # build validation summary
    vsum = make_validation_summary_from_df(df)
    vsum.update({"validator_passed": bool(is_valid), "validator_message": validation_msg})

    # 4) register file_registry (if DB engine available)
    engine = _get_db_engine()
    record_id = None
    if engine is not None:
        for attempt in range(1, cfg.pg_retries + 1):
            try:
                ensure_file_registry(engine)
                if not is_valid:
                    record_id = insert_file_record(engine, out_path.name, datetime.utcnow(), "validation_failed", vsum["rows_in"], vsum["duplicate_count"], vsum)
                    logging.error("Validation failed for %s: %s", out_path, validation_msg)
                    return 0  # stop early on validation failure
                else:
                    record_id = insert_file_record(engine, out_path.name, datetime.utcnow(), "uploaded", vsum["rows_in"], vsum["duplicate_count"], vsum)
                break
            except Exception as e:
                logging.warning("Postgres attempt %d failed: %s", attempt, e)
                time.sleep(cfg.pg_backoff ** (attempt - 1))
    else:
        logging.info("Database engine unavailable; skipping file_registry insertion (local only).")
        if not is_valid:
            logging.error("Validation failed but DB not available; stopping.")
            return 0

    # 5) upload to MinIO if requested
    if upload:
        ok, object_name = upload_with_retries(cfg, out_path)
        if not ok:
            logging.error("Upload failed for %s", out_path)
            if cfg.fail_on_upload_fail:
                raise RuntimeError("MinIO upload failed and fail_on_upload_fail set")
            # continue (local file preserved) — you can choose to stop here if you prefer
        else:
            logging.info("Upload OK: %s", object_name)

    # 6) optional: call existing upsert (which you said works in Airflow)
    if call_upsert_flag:
        try:
            result = call_project_upsert(out_path)
            # if the upsert function returns something useful, try to pick rows_out
            rows_out = None
            if isinstance(result, int):
                rows_out = result
            elif isinstance(result, dict):
                rows_out = result.get("rows_out")
            # update registry
            if engine is not None and record_id is not None:
                update_file_record_processed(engine, record_id, datetime.utcnow(), "processed", rows_out=rows_out, validation_summary=vsum)
        except Exception as e:
            logging.exception("Upsert/processing failed for %s: %s", out_path, e)
            if engine is not None and record_id is not None:
                try:
                    update_file_record_processed(engine, record_id, datetime.utcnow(), "processing_failed", rows_out=None, validation_summary={"error": str(e)})
                except Exception as ue:
                    logging.error("Failed to update registry after processing error: %s", ue)

    logging.info("Done. Local file at %s", out_path)
    return 0

# ---------- CLI shim (for testing or Airflow PythonOperator can call run) ----------
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Generate orders CSV, validate, optionally upload and call upsert.")
    p.add_argument("--num", type=int, default=1000)
    p.add_argument("--upload", action="store_true")
    p.add_argument("--call-upsert", action="store_true")
    p.add_argument("--parquet", action="store_true")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    try:
        rc = run(num=args.num, upload=args.upload, call_upsert_flag=args.call_upsert, write_parquet=args.parquet, quiet=args.quiet)
    except Exception as e:
        logging.exception("Generator failed: %s", e)
        raise
    else:
        raise SystemExit(rc)
