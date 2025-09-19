import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check

order_schema = DataFrameSchema({
    "order_id": Column(pa.String, nullable=False),
    "restaurant_id": Column(pa.Int, nullable=False, coerce=True),
    "customer_id": Column(pa.Int, nullable=False, coerce=True),
    "cuisine": Column(pa.String, nullable=False),
    "order_time": Column(pa.DateTime, nullable=False, coerce=True),
    "delivery_time": Column(pa.DateTime, nullable=False, coerce=True),
    "delivery_duration_minutes": Column(pa.Float, nullable=False, coerce=True, checks=Check.ge(0)),
    "distance_km": Column(pa.Float, nullable=True, coerce=True, checks=Check.ge(0).le(100)),
    "rating": Column(pa.Int, nullable=True, checks=Check.in_range(1, 5)),
    "amount": Column(pa.Float, nullable=True, coerce=True, checks=Check.ge(0)),
})

def validate_orders(csv_path):
    df = pd.read_csv(csv_path)
    try:
        validated_df = order_schema.validate(df)
        print(f"Validation passed for {csv_path}")
        return True, None
    except pa.errors.SchemaErrors as err:
        print(f"Validation failed for {csv_path}:\n{err}")
        return False, err

if __name__ == "__main__":
    import sys
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'data/incoming/orders.csv'
    validate_orders(csv_file)
