import pandas as pd
import os

def transform_orders(input_csv='data/incoming/orders.csv', output_csv='data/processed/orders_transformed.csv'):
    df = pd.read_csv(input_csv)

    # Normalize cuisine to lowercase (example)
    df['cuisine'] = df['cuisine'].str.lower()

    # Compute delivery_duration_minutes if missing or incorrect
    if 'delivery_duration_minutes' not in df.columns or df['delivery_duration_minutes'].isnull().any():
        df['order_time'] = pd.to_datetime(df['order_time'])
        df['delivery_time'] = pd.to_datetime(df['delivery_time'])
        df['delivery_duration_minutes'] = (df['delivery_time'] - df['order_time']).dt.total_seconds() / 60

    # Clean distance km to be positive floats
    df['distance_km'] = pd.to_numeric(df['distance_km'], errors='coerce').abs()

    # Fill missing ratings with median rating
    median_rating = df['rating'].median()
    df['rating'] = df['rating'].fillna(median_rating)

    # Save transformed data
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False)
    print(f"Transformed data saved to {output_csv}")

if __name__ == '__main__':
    transform_orders()
