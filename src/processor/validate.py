import warnings
import sys
import pandas as pd
import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check

# Suppress the FutureWarning from pandera about the DataFrameSchema import
warnings.simplefilter(action='ignore', category=FutureWarning)

# Define a schema for your order data validation
order_schema = DataFrameSchema({
    "order_id": Column(pa.String, nullable=False),
    "restaurant_id": Column(pa.Int64, nullable=False, coerce=True),
    "customer_id": Column(pa.Int64, nullable=False, coerce=True),
    "cuisine": Column(pa.String, nullable=False),
    "order_time": Column(pa.DateTime, nullable=False, coerce=True),
    "delivery_time": Column(pa.DateTime, nullable=False, coerce=True),
    "delivery_duration_minutes": Column(
        pa.Float64,
        nullable=False,
        coerce=True,
        checks=Check.ge(0)
    ),
    "distance_km": Column(
        pa.Float64,
        nullable=True,
        coerce=True,
        checks=Check.ge(0).le(100)
    ),
    "rating": Column(
        pa.Float64,
        nullable=True,
        coerce=True,
        checks=Check.in_range(1, 5)
    ),
    "amount": Column(
        pa.Float64,
        nullable=True,
        coerce=True,
        checks=Check.ge(0)
    ),
})

def validate_orders(csv_path: str) -> tuple[bool, pa.errors.SchemaErrors | None]:
    """
    Validates a CSV file of orders against a predefined pandera schema.

    Args:
        csv_path: The path to the CSV file to validate.

    Returns:
        A tuple containing a boolean indicating success and the SchemaErrors
        object if validation fails, otherwise None.
    """
    try:
        # Explicitly parse date columns when reading the CSV
        df = pd.read_csv(csv_path, parse_dates=['order_time', 'delivery_time'])
    except FileNotFoundError:
        print(f"Error: The file '{csv_path}' was not found.")
        return False, None
    
    try:
        # Use lazy=True to collect all errors instead of failing on the first one
        validated_df = order_schema.validate(df, lazy=True)
        print(f"Validation passed for {csv_path}")
        return True, None
    except pa.errors.SchemaErrors as err:
        print(f"Validation failed for {csv_path}:")
        # Print a concise summary of the failure cases
        print(err.failure_cases.to_string())
        return False, err

if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'data/incoming/orders.csv'
    
    is_valid, _ = validate_orders(csv_file)
    
    if not is_valid:
        # Exit with a non-zero status code to signal failure
        sys.exit(1)
