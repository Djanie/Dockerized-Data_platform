# src/processor/validate.py
import pandas as pd

def validate_orders(csv_path: str):
    try:
        df = pd.read_csv(csv_path, parse_dates=['order_time', 'delivery_time'])
        errors = []
        if df['order_id'].isnull().any():
            errors.append("Missing order IDs")
        if df['restaurant_id'].isnull().any():
            errors.append("Missing restaurant IDs")
        if df['customer_id'].isnull().any():
            errors.append("Missing customer IDs")
        if 'amount' in df.columns and df['amount'].notnull().any():
            if (df['amount'].dropna() < 0).any():
                errors.append("Negative amounts")
        if 'delivery_time' in df.columns and 'order_time' in df.columns:
            valid_times = df[['order_time', 'delivery_time']].dropna()
            if not valid_times.empty and not (valid_times['delivery_time'] > valid_times['order_time']).all():
                errors.append("Delivery before order")
        if errors:
            return False, ', '.join(errors)
        return True, None
    except Exception as e:
        return False, str(e)
