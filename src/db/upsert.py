# src/db/upsert.py
"""Simple upsert that uses your connection.get_db_engine() and does per-row INSERT ... ON CONFLICT.
Good for small/medium test runs. Keeps same orders table schema.
"""
from sqlalchemy import Table, MetaData, Column, String, Float, Integer, DateTime
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from src.db.connection import get_db_engine
import pandas as pd
from pathlib import Path

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
    Read CSV and upsert rows into orders table.
    Uses your get_db_engine() so it respects POSTGRES_HOST/PORT envs.
    """
    engine = get_db_engine()
    # ensure table exists
    metadata.create_all(engine)

    df = pd.read_csv(csv_path, parse_dates=["order_time", "delivery_time"])
    records = df.to_dict(orient="records")

    with engine.begin() as conn:  # transactional
        for rec in records:
            stmt = pg_insert(orders_table).values(**rec)
            update_dict = {c.name: stmt.excluded[c.name] for c in orders_table.c if c.name != "order_id"}
            upsert = stmt.on_conflict_do_update(index_elements=["order_id"], set_=update_dict)
            try:
                conn.execute(upsert)
            except SQLAlchemyError as e:
                # log and continue; small/simple behavior for quick runs
                print(f"Error upserting {rec.get('order_id')}: {e}")

if __name__ == "__main__":
    p = Path("data/processed/orders_transformed.csv")
    if not p.exists():
        print("Processed file not found:", p)
    else:
        upsert_orders(str(p))
        print("Upsert done.")
