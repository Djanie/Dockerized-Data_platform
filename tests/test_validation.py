import pytest
import pandas as pd
from src.processor.validate import validate_orders

# Create a sample CSV file for testing
@pytest.fixture
def sample_csv(tmp_path):
    data = {
        'order_id': ['id1', 'id2', None],
        'restaurant_id': [1, None, 3],
        'customer_id': [101, 102, 103],
        'amount': [10.5, -5.0, 15.0],
        'order_time': ['2023-01-01T10:00:00', '2023-01-02T10:00:00', '2023-01-03T09:00:00'],
        'delivery_time': ['2023-01-01T11:00:00', '2023-01-02T09:00:00', '2023-01-03T10:00:00']
    }
    df = pd.DataFrame(data)
    csv_path = tmp_path / "test_orders.csv"
    df.to_csv(csv_path, index=False)
    return csv_path

def test_validate_orders_success(sample_csv):
    success, message = validate_orders(str(sample_csv))
    assert success is False  # Expect failure due to negative amount and delivery before order
    assert "Negative amounts, Delivery before order" in message

def test_validate_orders_missing_ids(sample_csv):
    # Modify CSV to test missing IDs
    df = pd.read_csv(sample_csv)
    df.loc[0, 'order_id'] = None
    df.to_csv(sample_csv, index=False)
    success, message = validate_orders(str(sample_csv))
    assert success is False
    assert "Missing order IDs" in message

def test_validate_orders_invalid_file(tmp_path):
    invalid_path = tmp_path / "nonexistent.csv"
    success, message = validate_orders(str(invalid_path))
    assert success is False
    assert "No such file or directory" in message