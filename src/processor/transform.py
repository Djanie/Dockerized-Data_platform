# src/processor/transform.py
"""Module for transforming order data from CSV files."""

import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_orders(input_csv='data/incoming/orders.csv', output_csv='data/processed/orders_transformed.csv'):
    """
    Transform order data by normalizing fields and handling missing values.

    Args:
        input_csv (str): Path to the input CSV file containing order data.
        output_csv (str): Path to save the transformed CSV file.
    """
    df = pd.read_csv(input_csv)

    # Normalize cuisine to lowercase
    df['cuisine'] = df['cuisine'].str.lower()

    # Compute delivery_duration_minutes if missing or incorrect
    if 'delivery_duration_minutes' not in df.columns or df['delivery_duration_minutes'].isnull().any():
        df['order_time'] = pd.to_datetime(df['order_time'])
        df['delivery_time'] = pd.to_datetime(df['delivery_time'])
        df['delivery_duration_minutes'] = (df['delivery_time'] - df['order_time']).dt.total_seconds() / 60

    # Clean distance_km to be positive floats
    df['distance_km'] = pd.to_numeric(df['distance_km'], errors='coerce').abs()

    # Fill missing ratings with median rating
    median_rating = df['rating'].median()
    df['rating'] = df['rating'].fillna(median_rating)

    # Save transformed data
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False)
    logger.info("Transformed data saved to %s", output_csv)

if __name__ == '__main__':
    """Execute transformation process for testing purposes."""
    transform_orders()