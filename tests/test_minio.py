import pytest
from minio import Minio
from src.generator.generate_data import upload_with_retries, _MinioLazy
import os
from pathlib import Path

# Mock MinIO client and environment
@pytest.fixture
def mock_minio_config(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "minioadmin")
    monkeypatch.setenv("MINIO_BUCKET", "testbucket")

@pytest.fixture
def mock_minio_client(mocker):
    mock_client = mocker.Mock(spec=Minio)
    mock_client.bucket_exists.return_value = False
    mock_client.make_bucket.return_value = None
    mock_client.fput_object.return_value = None
    mocker.patch("src.generator.generate_data.Minio", return_value=mock_client)
    return mock_client

def test_upload_with_retries_success(mock_minio_config, mock_minio_client):
    cfg = type('CFG', (), {"minio_endpoint": "localhost:9000", "minio_access": "minioadmin", 
                           "minio_secret": "minioadmin", "minio_bucket": "testbucket", 
                           "minio_retries": 1, "minio_backoff": 1})()
    temp_file = Path("test_data.csv")
    temp_file.touch()
    
    success, obj_name = upload_with_retries(cfg, temp_file)
    assert success is True
    assert obj_name == "test_data.csv"
    mock_minio_client.make_bucket.assert_called_once_with("testbucket")
    mock_minio_client.fput_object.assert_called_once_with("testbucket", "test_data.csv", str(temp_file))
    
    temp_file.unlink()

def test_upload_with_retries_failure(mock_minio_config, mocker):
    cfg = type('CFG', (), {"minio_endpoint": "localhost:9000", "minio_access": "minioadmin", 
                           "minio_secret": "minioadmin", "minio_bucket": "testbucket", 
                           "minio_retries": 1, "minio_backoff": 1})()
    mocker.patch("src.generator.generate_data._MinioLazy.upload_file", side_effect=Exception("Upload failed"))
    
    success, obj_name = upload_with_retries(cfg, Path("test_data.csv"))
    assert success is False
    assert obj_name is None