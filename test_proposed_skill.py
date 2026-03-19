import sys
from proposed_skill import *

import pytest
import requests
import json
import os
from unittest.mock import MagicMock, patch

# NOTE: The 'proposed_skill' import is added automatically by the testing framework.
# For example, extract_data, transform_data, etc., will be available directly.

@pytest.fixture
def temp_output_file(tmp_path):
    """Fixture to create a temporary file path for output and ensure cleanup."""
    file_path = tmp_path / "output.json"
    yield str(file_path)
    if os.path.exists(file_path):
        os.remove(file_path)

def test_extract_data_success():
    """Tests successful data extraction from an API."""
    mock_response = MagicMock()
    mock_response.json.return_value = [{"id": 1, "name": "Test User", "status": "active"}]
    mock_response.raise_for_status.return_value = None
    with patch('requests.get', return_value=mock_response) as mock_get:
        data = extract_data("http://test.api/users")
        mock_get.assert_called_once_with("http://test.api/users")
        assert data == [{"id": 1, "name": "Test User", "status": "active"}]

def test_extract_data_http_error():
    """Tests data extraction failure due to an HTTP error."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
    with patch('requests.get', return_value=mock_response):
        with pytest.raises(requests.exceptions.HTTPError):
            extract_data("http://test.api/nonexistent")

def test_extract_data_connection_error():
    """Tests data extraction failure due to a connection error."""
    with patch('requests.get', side_effect=requests.exceptions.ConnectionError("Connection failed")):
        with pytest.raises(requests.exceptions.ConnectionError):
            extract_data("http://bad.api/users")

def test_transform_data_basic_filtering():
    """Tests transformation with active user filtering and field selection."""
    raw_data = [
        {"id": 1, "name": "Alice", "status": "active", "age": 30},
        {"id": 2, "name": "Bob", "status": "inactive", "age": 25},
        {"id": 3, "name": "Charlie", "status": "active", "age": 35}
    ]
    expected_data = [
        {"user_id": 1, "user_name": "Alice"},
        {"user_id": 3, "user_name": "Charlie"}
    ]
    transformed = transform_data(raw_data)
    assert transformed == expected_data

def test_transform_data_no_active_users():
    """Tests transformation when no active users are present."""
    raw_data = [
        {"id": 1, "name": "Alice", "status": "inactive"},
        {"id": 2, "name": "Bob", "status": "pending"}
    ]
    transformed = transform_data(raw_data)
    assert transformed == []

def test_transform_data_empty_input():
    """Tests transformation with an empty input list."""
    transformed = transform_data([])
    assert transformed == []

def test_transform_data_malformed_records():
    """Tests transformation with malformed records (missing keys, non-dict items)."""
    raw_data = [
        {"id": 1, "name": "Alice", "status": "active"},
        {"id": 2, "status": "active"}, # Missing name
        {"name": "Charlie", "status": "active"}, # Missing id
        "not_a_dict" # Invalid record type
    ]
    expected_data = [
        {"user_id": 1, "user_name": "Alice"},
        {"user_id": 2, "user_name": None}, # 'name' will be None if not found
        {"user_id": None, "user_name": "Charlie"} # 'id' will be None if not found
    ]
    transformed = transform_data(raw_data)
    assert transformed == expected_data

def test_transform_data_non_list_input():
    """Tests transformation with a non-list input, expecting a warning and empty list."""
    with patch('builtins.print') as mock_print:
        transformed = transform_data({"id": 1, "name": "Alice", "status": "active"})
        assert transformed == []
        mock_print.assert_any_call("Warning: raw_data is not a list. Returning empty list.")

def test_load_data_success(temp_output_file):
    """Tests successful data loading to a JSON file."""
    data_to_load = [{"user_id": 1, "user_name": "Alice"}]
    load_data(data_to_load, temp_output_file)
    with open(temp_output_file, 'r', encoding='utf-8') as f:
        loaded_data = json.load(f)
    assert loaded_data == data_to_load

def test_run_simple_etl_pipeline_success(temp_output_file):
    """Tests the end-to-end pipeline for success."""
    mock_raw_data = [
        {"id": 101, "name": "User A", "status": "active"},
        {"id": 102, "name": "User B", "status": "inactive"},
        {"id": 103, "name": "User C", "status": "active"}
    ]
    mock_processed_data = [
        {"user_id": 101, "user_name": "User A"},
        {"user_id": 103, "user_name": "User C"}
    ]

    # The original comma-separated 'with' statements sometimes cause SyntaxError
    # with older pytest versions or specific AST parsing configurations.
    # Nesting them is a more robust approach.
    with patch('requests.get') as mock_get:
        with patch('builtins.open', MagicMock()) as mock_open:
            with patch('json.dump', MagicMock()) as mock_json_dump:
                with patch('builtins.print') as mock_print: # Suppress prints during test

                    mock_get.return_value.json.return_value = mock_raw_data
                    mock_get.return_value.raise_for_status.return_value = None

                    run_simple_etl_pipeline("http://test.api/pipeline", temp_output_file)

                    mock_get.assert_called_once_with("http://test.api/pipeline")
                    mock_open.assert_called_once_with(temp_output_file, 'w', encoding='utf-8')
                    # Corrected assertion: `mock_open.return_value` refers to the mocked file handle.
                    mock_json_dump.assert_called_once_with(mock_processed_data, mock_open.return_value, indent=4)
                    mock_print.assert_any_call("Starting ETL pipeline for API: http://test.api/pipeline")
                    mock_print.assert_any_call("Extracted 3 records.")
                    mock_print.assert_any_call("Transformed 2 records (active users).")
                    mock_print.assert_any_call(f"Data successfully loaded to {temp_output_file}")
                    mock_print.assert_any_call("ETL pipeline completed successfully.")


def test_run_simple_etl_pipeline_failure_extract(temp_output_file):
    """Tests pipeline failure during the extraction phase."""
    with patch('requests.get', side_effect=requests.exceptions.RequestException("API error")):
        with patch('builtins.print') as mock_print:
            with pytest.raises(requests.exceptions.RequestException):
                run_simple_etl_pipeline("http://test.api/fail", temp_output_file)
            mock_print.assert_any_call("ETL pipeline failed: API error")

def test_run_simple_etl_pipeline_failure_load(temp_output_file):
    """Tests pipeline failure during the loading phase."""
    mock_raw_data = [{"id": 1, "name": "A", "status": "active"}]
    # The original comma-separated 'with' statements sometimes cause SyntaxError.
    # Nesting them is a more robust approach.
    with patch('requests.get') as mock_get:
        with patch('builtins.open', side_effect=IOError("Permission denied")):
            with patch('builtins.print') as mock_print:

                mock_get.return_value.json.return_value = mock_raw_data
                mock_get.return_value.raise_for_status.return_value = None

                with pytest.raises(IOError):
                    run_simple_etl_pipeline("http://test.api/success", "/invalid/path/output.json")
                mock_print.assert_any_call("ETL pipeline failed: Permission denied")
