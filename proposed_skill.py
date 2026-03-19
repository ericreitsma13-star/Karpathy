import requests
import json
import os
from typing import List, Dict, Any

def extract_data(api_url: str) -> List[Dict[str, Any]]:
    """
    Extracts data from a given API URL.
    Args:
        api_url: The URL of the API to fetch data from.
    Returns:
        A list of dictionaries, where each dictionary is a record from the API.
    Raises:
        requests.exceptions.RequestException: If the API call fails.
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error extracting data from API {api_url}: {e}")
        raise

def transform_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transforms raw data by filtering and selecting specific fields.
    Example: Filters records where 'status' is 'active' and selects 'id' and 'name'.
    Args:
        raw_data: A list of dictionaries (raw records).
    Returns:
        A list of dictionaries with transformed records.
    """
    if not isinstance(raw_data, list):
        print("Warning: raw_data is not a list. Returning empty list.")
        return []

    transformed = []
    for record in raw_data:
        if isinstance(record, dict) and record.get('status') == 'active':
            transformed.append({
                'user_id': record.get('id'),
                'user_name': record.get('name')
            })
    return transformed

def load_data(processed_data: List[Dict[str, Any]], target_file_path: str) -> None:
    """
    Loads processed data into a JSON file.
    Args:
        processed_data: A list of dictionaries (processed records).
        target_file_path: The path to the target JSON file.
    """
    try:
        with open(target_file_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=4)
        print(f"Data successfully loaded to {target_file_path}")
    except IOError as e:
        print(f"Error loading data to file {target_file_path}: {e}")
        raise

def run_simple_etl_pipeline(api_url: str, output_file_path: str) -> None:
    """
    Orchestrates a simple Extract-Transform-Load pipeline.
    Args:
        api_url: The URL to extract data from.
        output_file_path: The file path to load transformed data into.
    """
    print(f"Starting ETL pipeline for API: {api_url}")
    try:
        # Extract
        raw_data = extract_data(api_url)
        print(f"Extracted {len(raw_data)} records.")

        # Transform
        processed_data = transform_data(raw_data)
        print(f"Transformed {len(processed_data)} records (active users).")

        # Load
        load_data(processed_data, output_file_path)
        print("ETL pipeline completed successfully.")
    except Exception as e:
        print(f"ETL pipeline failed: {e}")
        raise
