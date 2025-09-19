from sqlalchemy import Table, MetaData, Column, String, Float, Integer, DateTime
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
import os
import pandas as pd

metadata = MetaData()

# Define table schema for SQLAlchemy
orders_table = Table('orders', metadata,
    Column('order_id', String, primary_key=True),
    Column('restaurant_id', Integer, nullable=False),
    Column('customer_id', Integer, nullable=False),
    Column('cuisine', String, nullable=False),
    Column('order_time', DateTime, nullable=False),
    Column('delivery_time', DateTime, nullable=False),
    Column('delivery_duration_minutes', Float, nullable=False),
    Column('distance_km', Float, nullable=True),
    Column('rating', Integer, nullable=True),
    Column('amount', Float, nullable=True)
)

def get_engine():
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5433')
    dbname = os.getenv('POSTGRES_DB', 'orders_db')

    db_url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
    return create_engine(db_url)

def upsert_orders(csv_path):
    engine = get_engine()
    df = pd.read_csv(csv_path)

    rows = df.to_dict(orient='records')

    with engine.connect() as conn:
        for row in rows:
            stmt = insert(orders_table).values(**row)
            do_update_stmt = stmt.on_conflict_do_update(
                index_elements=['order_id'],
                set_={key: stmt.excluded[key] for key in row.keys()}
            )
            try:
                conn.execute(do_update_stmt)
            except SQLAlchemyError as e:
                print(f"Error upserting row {row['order_id']}: {e}")

if __name__ == "__main__":
    upsert_orders('data/processed/orders_transformed.csv')
    print("Upsert of orders done.")
