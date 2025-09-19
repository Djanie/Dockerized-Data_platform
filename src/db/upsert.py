from sqlalchemy import Table, MetaData, Column, String, Float, Integer, DateTime
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
import os
import pandas as pd

metadata = MetaData()

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
    # Use Docker service name, not localhost
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    host = 'postgres'  # Docker service name
    port = '5432'      # Internal container port
    dbname = os.getenv('POSTGRES_DB', 'orders_db')

    db_url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
    engine = create_engine(db_url)
    return engine
def upsert_orders(csv_path):
    engine = get_engine()
    # Create table if it doesn't exist
    metadata.create_all(engine)

    df = pd.read_csv(csv_path)
    rows = df.to_dict(orient='records')

    with engine.connect() as conn:
        for row in rows:
            stmt = insert(orders_table).values(**row)
            do_update_stmt = stmt.on_conflict_do_update(
                index_elements=['order_id'],
                set_={key: stmt.excluded[key] for key in row.keys()}
            )
            trans = conn.begin()
            try:
                conn.execute(do_update_stmt)
                trans.commit()
            except SQLAlchemyError as e:
                trans.rollback()
                print(f"Error upserting row {row['order_id']}: {e}")

if __name__ == "__main__":
    upsert_orders('data/processed/orders_transformed.csv')
    print("Upsert of orders done.")
