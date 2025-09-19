from sqlalchemy import create_engine, text
import os

def get_db_engine():
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    host = os.getenv('POSTGRES_HOST', 'localhost')   # or 'postgres' if inside docker
    port = os.getenv('POSTGRES_PORT', '5433')
    dbname = os.getenv('POSTGRES_DB', 'orders_db')

    db_url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
    engine = create_engine(db_url)
    return engine

if __name__ == "__main__":
    engine = get_db_engine()
    with engine.connect() as connection:
        result = connection.execute(text("SELECT version();"))
        version = result.fetchone()
        print(f"Connected to PostgreSQL version: {version[0]}")
