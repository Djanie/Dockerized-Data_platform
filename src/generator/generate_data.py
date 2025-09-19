#!/usr/bin/env python3
"""
generate_and_upload.py

Usage (examples):
  python generate_and_upload.py --num-records 500 --out-dir data/incoming
  MINIO_ENDPOINT=localhost:9000 MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
    python generate_and_upload.py --upload

This script:
 - Generates synthetic online food order data using Faker.
 - Writes to a timestamped file (CSV by default, Parquet optional).
 - Optionally uploads the file to MinIO with retries and configurable bucket/name.
 - Returns non-zero exit (raises) if upload fails when --fail-on-upload-fail is set.
"""
from __future__ import annotations
import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd
from faker import Faker
from minio import Minio
from minio.error import S3Error

# ------------- Configuration dataclass -------------
@dataclass
class Config:
    out_dir: Path
    filename_prefix: str = "orders"
    num_records: int = 1000
    seed: Optional[int] = None
    cuisines: tuple = ("Italian", "Chinese", "Mexican", "Indian", "American")
    duplicate_frac: float = 0.05
    null_rating_frac: float = 0.02
    write_parquet: bool = False
    parquet_engine: Optional[str] = None  # e.g. "pyarrow" or "fastparquet"

    # MinIO options
    minio_endpoint: Optional[str] = None
    minio_access_key: Optional[str] = None
    minio_secret_key: Optional[str] = None
    minio_secure: bool = False
    minio_bucket: str = "incoming"
    minio_object_name: Optional[str] = None  # if None -> auto from filename
    minio_max_retries: int = 3
    minio_backoff_base: int = 2  # seconds

    fail_on_upload_fail: bool = False
    quiet: bool = False

# ------------- Utilities & Logging -------------
def configure_logging(quiet: bool):
    level = logging.WARNING if quiet else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

# ------------- Data generation -------------
def generate_orders_df(cfg: Config) -> pd.DataFrame:
    """
    Generate a DataFrame containing synthetic order records.

    Returns:
        pd.DataFrame: generated orders (may contain duplicates as configured)
    """
    logging.info("Generating %d records (seed=%s)...", cfg.num_records, cfg.seed)
    fake = Faker()
    if cfg.seed is not None:
        Faker.seed(cfg.seed)

    rows = []
    import random
    for _ in range(cfg.num_records):
        order_time = fake.date_time_this_year()
        # ensure delivery_time is equal or after order_time
        delivery_time = fake.date_time_between(start_date=order_time)
        delivery_duration_mins = (delivery_time - order_time).total_seconds() / 60.0

        record = {
            "order_id": fake.uuid4(),
            "restaurant_id": random.randint(1, 100),
            "customer_id": random.randint(1, 500),
            "cuisine": random.choice(cfg.cuisines),
            "order_time": order_time,
            "delivery_time": delivery_time,
            "delivery_duration_minutes": round(delivery_duration_mins, 2),
            "distance_km": round(random.uniform(0.1, 20.0), 2),
            # rating: sometimes None
            "rating": random.choice([1, 2, 3, 4, 5, None]),
            "amount": round(random.uniform(10.0, 200.0), 2),
        }
        rows.append(record)

    df = pd.DataFrame(rows)

    # Force some null ratings at the exact configured fraction (overrides random)
    if cfg.null_rating_frac and 0.0 < cfg.null_rating_frac < 1.0:
        n_nulls = int(len(df) * cfg.null_rating_frac)
        if n_nulls > 0:
            null_idx = df.sample(n=n_nulls, random_state=cfg.seed or 0).index
            df.loc[null_idx, "rating"] = None

    # Inject duplicates (full-row duplicates) to simulate dirty data
    if cfg.duplicate_frac and 0.0 < cfg.duplicate_frac < 1.0:
        n_dups = int(len(df) * cfg.duplicate_frac)
        if n_dups > 0:
            dup_rows = df.sample(n=n_dups, replace=True, random_state=cfg.seed or 1)
            df = pd.concat([df, dup_rows], ignore_index=True)

    logging.info("Generated DataFrame with %d rows (including duplicates).", len(df))
    return df

# ------------- File writing -------------
def make_out_filename(cfg: Config, ext: str) -> Path:
    """
    Create a timestamped filename; ensures no overwrite by adding a counter if needed.
    """
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = f"{cfg.filename_prefix}_{ts}"
    filename = f"{base}.{ext}"
    out_path = cfg.out_dir / filename

    # avoid overwriting: append counter if file exists (very unlikely with seconds timestamp)
    counter = 1
    while out_path.exists():
        filename = f"{base}_{counter}.{ext}"
        out_path = cfg.out_dir / filename
        counter += 1
    return out_path

def write_dataframe(cfg: Config, df: pd.DataFrame) -> Path:
    """
    Write DataFrame to Parquet (preferred) or CSV. Returns path written.
    """
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    if cfg.write_parquet:
        ext = "parquet"
        path = make_out_filename(cfg, ext)
        try:
            # attempt parquet write
            df.to_parquet(path, index=False, engine=cfg.parquet_engine)
            logging.info("Wrote parquet to %s", path)
            return path
        except Exception as e:
            logging.warning("Parquet write failed (%s). Falling back to CSV.", e)
            # fallthrough to CSV

    # CSV fallback
    ext = "csv"
    path = make_out_filename(cfg, ext)
    df.to_csv(path, index=False)
    logging.info("Wrote CSV to %s", path)
    return path

# ------------- MinIO helper (improved) -------------
class MinioHelperSimple:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False):
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

    def ensure_bucket(self, bucket_name: str) -> None:
        if not self.client.bucket_exists(bucket_name):
            logging.info("Bucket '%s' does not exist. Creating...", bucket_name)
            self.client.make_bucket(bucket_name)

    def upload_file(self, bucket_name: str, object_name: str, file_path: Path) -> None:
        """
        Uploads a file to MinIO. Raises S3Error or FileNotFoundError on failure.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File to upload not found: {file_path}")
        self.client.fput_object(bucket_name, object_name, str(file_path))
        logging.info("Uploaded %s -> %s/%s", file_path, bucket_name, object_name)

def upload_with_retries(
    cfg: Config, minio_helper: MinioHelperSimple, local_path: Path
) -> bool:
    """
    Try upload with retries and exponential backoff. Returns True on success, False on final failure.
    """
    bucket = cfg.minio_bucket
    object_name = cfg.minio_object_name or local_path.name
    last_exc: Optional[Exception] = None

    for attempt in range(1, cfg.minio_max_retries + 1):
        try:
            minio_helper.ensure_bucket(bucket)
            minio_helper.upload_file(bucket, object_name, local_path)
            return True
        except (S3Error, FileNotFoundError, OSError) as e:
            logging.warning("Upload attempt %d/%d failed: %s", attempt, cfg.minio_max_retries, e)
            last_exc = e
            backoff = cfg.minio_backoff_base ** (attempt - 1)
            time.sleep(backoff)

    logging.error("All upload attempts failed. Last error: %s", last_exc)
    return False

# ------------- CLI / main -------------
def build_config_from_args(args: argparse.Namespace) -> Config:
    # env fallbacks for MinIO
    endpoint = args.minio_endpoint or os.getenv("MINIO_ENDPOINT")
    access = args.minio_access_key or os.getenv("MINIO_ROOT_USER")
    secret = args.minio_secret_key or os.getenv("MINIO_ROOT_PASSWORD")
    secure = bool(os.getenv("MINIO_SECURE", "")).__bool__() if args.minio_secure is None else args.minio_secure

    cfg = Config(
        out_dir=Path(args.out_dir),
        filename_prefix=args.filename_prefix,
        num_records=args.num_records,
        seed=args.seed,
        duplicate_frac=args.duplicate_frac,
        null_rating_frac=args.null_rating_frac,
        write_parquet=args.write_parquet,
        parquet_engine=args.parquet_engine,
        minio_endpoint=endpoint,
        minio_access_key=access,
        minio_secret_key=secret,
        minio_secure=secure,
        minio_bucket=args.minio_bucket,
        minio_object_name=args.minio_object_name,
        minio_max_retries=args.minio_max_retries,
        minio_backoff_base=args.minio_backoff_base,
        fail_on_upload_fail=args.fail_on_upload_fail,
        quiet=args.quiet,
    )
    return cfg

def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic orders and optionally upload to MinIO.")
    p.add_argument("--out-dir", default="data/incoming", help="Local directory to write files")
    p.add_argument("--filename-prefix", default="orders", help="Filename prefix")
    p.add_argument("--num-records", type=int, default=1000, help="Number of base records to generate")
    p.add_argument("--seed", type=int, default=None, help="Faker / RNG seed (optional for reproducibility)")
    p.add_argument("--write-parquet", action="store_true", help="Write parquet instead of CSV (requires pyarrow/fastparquet)")
    p.add_argument("--parquet-engine", default=None, help="Parquet engine (pyarrow or fastparquet)")

    # data quirks
    p.add_argument("--duplicate-frac", type=float, default=0.05, help="Fraction of duplicate rows to inject")
    p.add_argument("--null-rating-frac", type=float, default=0.02, help="Fraction of null ratings to force")

    # MinIO options (CLI or via env)
    p.add_argument("--upload", action="store_true", help="Upload generated file to MinIO (requires MinIO config)")
    p.add_argument("--minio-endpoint", default=None, help="MinIO endpoint (or use MINIO_ENDPOINT env)")
    p.add_argument("--minio-access-key", default=None, help="MinIO access key (or MINIO_ROOT_USER env)")
    p.add_argument("--minio-secret-key", default=None, help="MinIO secret key (or MINIO_ROOT_PASSWORD env)")
    p.add_argument("--minio-secure", type=bool, default=False, help="Use TLS for MinIO (bool)")
    p.add_argument("--minio-bucket", default="incoming", help="Bucket to upload to")
    p.add_argument("--minio-object-name", default=None, help="Object name in bucket (defaults to filename)")
    p.add_argument("--minio-max-retries", type=int, default=3, help="Upload retry attempts")
    p.add_argument("--minio-backoff-base", type=int, default=2, help="Exponential backoff base seconds")

    p.add_argument("--fail-on-upload-fail", action="store_true", help="Exit non-zero if upload ultimately fails")
    p.add_argument("--quiet", action="store_true", help="Reduce output logging")
    return p.parse_args(argv)

def main(argv: Optional[list] = None) -> int:
    args = parse_args(argv)
    cfg = build_config_from_args(args)
    configure_logging(cfg.quiet)

    logging.info("Config: %s", cfg)

    df = generate_orders_df(cfg)
    out_path = write_dataframe(cfg, df)

    # Upload if requested
    if args.upload:
        # validate minio config
        if not cfg.minio_endpoint or not cfg.minio_access_key or not cfg.minio_secret_key:
            logging.error("Missing MinIO configuration. Provide via CLI args or env vars (MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD).")
            if cfg.fail_on_upload_fail:
                raise RuntimeError("Missing MinIO configuration.")
            return 2

        helper = MinioHelperSimple(cfg.minio_endpoint, cfg.minio_access_key, cfg.minio_secret_key, secure=cfg.minio_secure)
        success = upload_with_retries(cfg, helper, out_path)

        if not success:
            if cfg.fail_on_upload_fail:
                raise RuntimeError("Upload failed after retries.")
            logging.warning("Upload failed, but --fail-on-upload-fail not set. Local file preserved at %s", out_path)
        else:
            logging.info("Upload succeeded.")

    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
    except Exception as e:
        logging.exception("Script failed: %s", e)
        sys.exit(1)
    else:
        sys.exit(exit_code)
