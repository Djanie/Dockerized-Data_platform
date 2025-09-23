# src/db/upsert.py
"""Module for performing upsert operations on the orders table using SQLAlchemy."""

from sqlalchemy import Table, MetaData, Column, String, Float, Integer, DateTime
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from src.db.connection import get_db_engine
import pandas as pd
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

metadata = MetaData()

orders_table = Table(
    "orders",
    metadata,
    Column("order_id", String, primary_key=True),
    Column("restaurant_id", Integer, nullable=False),
    Column("customer_id", Integer, nullable=False),
    Column("cuisine", String, nullable=False),
    Column("order_time", DateTime, nullable=False),
    Column("delivery_time", DateTime, nullable=False),
    Column("delivery_duration_minutes", Float, nullable=False),
    Column("distance_km", Float, nullable=True),
    Column("rating", Integer, nullable=True),
    Column("amount", Float, nullable=True),
)

def upsert_orders(csv_path: str):
    """
    Read CSV data and perform upsert operations into the orders table.

    Args:
        csv_path (str): Path to the CSV file containing order data.

    Raises:
        FileNotFoundError: If the specified CSV file does not exist.
        SQLAlchemyError: If an error occurs during database operations.
    """
    engine = get_db_engine()
    metadata.create_all(engine)

    try:
        df = pd.read_csv(csv_path, parse_dates=["order_time", "delivery_time"])
        records = df.to_dict(orient="records")

        with engine.begin() as conn:
            for rec in records:
                stmt = pg_insert(orders_table).values(**rec)
                update_dict = {c.name: stmt.excluded[c.name] for c in orders_table.c if c.name != "order_id"}
                upsert = stmt.on_conflict_do_update(index_elements=["order_id"], set_=update_dict)
                try:
                    conn.execute(upsert)
                except SQLAlchemyError as e:
                    logger.warning("Error upserting record %s: %s", rec.get('order_id'), e)
                    continue
        logger.info("Upsert operation completed successfully for %s", csv_path)
    except FileNotFoundError as e:
        logger.error("CSV file not found: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected error during upsert: %s", e)
        raise

if __name__ == "__main__":
    """Execute upsert operation for testing purposes."""
    try:
        p = Path("data/processed/orders_transformed.csv")
        if not p.exists():
            logger.error("Processed file not found: %s", p)
            raise FileNotFoundError(f"{p} not found")
        upsert_orders(str(p))
        logger.info("Upsert operation completed")
    except Exception as e:
        logger.error("Test execution failed: %s", e)
        raise