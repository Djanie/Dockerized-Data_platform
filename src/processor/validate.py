# src/processor/validate.py
"""Module for validating order data in CSV files."""

import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_orders(csv_path: str):
    """
    Validate the integrity of order data in a CSV file.

    Args:
        csv_path (str): Path to the CSV file to validate.

    Returns:
        Tuple[bool, Optional[str]]: A tuple containing a boolean indicating success and an error message if validation fails.
    """
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
            error_message = ', '.join(errors)
            logger.warning("Validation failed: %s", error_message)
            return False, error_message
        logger.info("Validation succeeded for %s", csv_path)
        return True, None
    except Exception as e:
        logger.error("Validation failed due to exception: %s", str(e))
        return False, str(e)