import pandas as pd

def validate_orders(csv_path: str):
    try:
        df = pd.read_csv(csv_path, parse_dates=['order_time', 'delivery_time'])
        errors = []
        
        if df.isnull().sum().sum() > 0:
            errors.append("Null values found")
        if not (df['delivery_time'] > df['order_time']).all():
            errors.append("Delivery time before order time")
        if (df['amount'] < 0).any():
            errors.append("Negative amounts")
            
        if errors:
            return False, ', '.join(errors)
        return True, None
    except Exception as e:
        return False, str(e)