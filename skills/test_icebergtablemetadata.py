import sys
from skills.icebergtablemetadata import *

import pytest
import time
from typing import List, Dict, Any

# The classes DataFile, ManifestFile, ManifestList, Snapshot, TableSchema,
# and IcebergTableMetadata are automatically available from proposed_skill.

def test_initialization():
    initial_schema = [{'name': 'id', 'type': 'int'}, {'name': 'name', 'type': 'string'}]
    table = IcebergTableMetadata("my_iceberg_table", initial_schema)

    assert table.table_name == "my_iceberg_table"
    assert table._current_snapshot_id is not None
    assert len(table._snapshots) == 1
    assert len(table._schemas) == 1
    assert len(table._manifest_files) == 1 # Empty manifest file for initial state
    assert len(table._manifest_lists) == 1 # Empty manifest list for initial state

    state = table.get_table_state()
    assert state['table_name'] == "my_iceberg_table"
    assert state['schema']['fields'] == initial_schema
    assert state['data_files'] == []
    assert state['total_rows'] == 0
    assert state['operation'] == 'create'

def test_append_data():
    initial_schema = [{'name': 'id', 'type': 'int'}, {'name': 'name', 'type': 'string'}]
    table = IcebergTableMetadata("my_iceberg_table", initial_schema)

    # First append
    data_files1 = [
        DataFile("path/to/data1.parquet", 100, 10, {'id': {'min': 1, 'max': 10}}),
        DataFile("path/to/data2.parquet", 150, 15, {'id': {'min': 11, 'max': 25}})
    ]
    snapshot_id1 = table.append_data(data_files1)
    
    assert table._current_snapshot_id == snapshot_id1
    assert len(table._snapshots) == 2 # Initial + first append
    assert len(table._manifest_files) == 2 # Initial empty + new manifest for data_files1
    assert len(table._manifest_lists) == 2 # Initial empty + new list for data_files1

    state1 = table.get_table_state(snapshot_id1)
    assert len(state1['data_files']) == 2
    assert state1['total_rows'] == 25
    assert state1['operation'] == 'append'

    # Second append
    data_files2 = [
        DataFile("path/to/data3.parquet", 200, 20, {'id': {'min': 26, 'max': 45}})
    ]
    snapshot_id2 = table.append_data(data_files2)

    assert table._current_snapshot_id == snapshot_id2
    assert len(table._snapshots) == 3 # Initial + append1 + append2
    assert len(table._manifest_files) == 3 # Initial + mf1 + mf2
    assert len(table._manifest_lists) == 3 # Initial + ml1 + ml2

    state2 = table.get_table_state(snapshot_id2)
    assert len(state2['data_files']) == 3 # Both sets of data files
    assert state2['total_rows'] == 25 + 20 # Sum of all rows
    assert state2['operation'] == 'append'

    # Check time travel to snapshot 1
    state_historical = table.get_table_state(snapshot_id1)
    assert len(state_historical['data_files']) == 2
    assert state_historical['total_rows'] == 25
    assert state_historical['current_snapshot_id'] == snapshot_id1

def test_update_schema():
    initial_schema = [{'name': 'id', 'type': 'int'}]
    table = IcebergTableMetadata("my_iceberg_table", initial_schema)

    # Append some data first
    data_files1 = [DataFile("path/to/data1.parquet", 100, 10, {'id': {'min': 1, 'max': 10}})]
    # Use time.sleep to ensure distinct timestamps for snapshots
    time.sleep(0.01)
    _ = table.append_data(data_files1) # Snapshot 1 (append)

    current_state_before_schema_update = table.get_table_state()
    assert current_state_before_schema_update['schema']['fields'] == initial_schema
    assert len(current_state_before_schema_update['data_files']) == 1

    # Update schema
    new_schema = [
        {'name': 'id', 'type': 'long'},
        {'name': 'description', 'type': 'string'}
    ]
    time.sleep(0.01)
    snapshot_id_schema_update = table.update_schema(new_schema) # Snapshot 2 (schema_update)

    assert table._current_snapshot_id == snapshot_id_schema_update
    assert len(table._schemas) == 2 # Initial + new schema
    assert len(table._snapshots) == 3 # Initial + append + schema_update

    state_after_schema_update = table.get_table_state(snapshot_id_schema_update)
    assert state_after_schema_update['schema']['fields'] == new_schema
    # Data files remain the same, only the schema interpretation changes
    assert state_after_schema_update['data_files'] == current_state_before_schema_update['data_files'] 
    assert state_after_schema_update['total_rows'] == 10
    assert state_after_schema_update['operation'] == 'schema_update'

    # Verify time travel to before schema update
    # Get the ID of the 'append' snapshot dynamically
    first_append_snapshot_id = next(s['snapshot_id'] for s in table.get_history() if s['operation'] == 'append')
    state_before_schema_update = table.get_table_state(first_append_snapshot_id)
    assert state_before_schema_update['schema']['fields'] == initial_schema
    assert len(state_before_schema_update['data_files']) == 1
    assert state_before_schema_update['operation'] == 'append'


def test_serialization_deserialization():
    initial_schema = [{'name': 'col_a', 'type': 'int'}]
    table = IcebergTableMetadata("my_serial_table", initial_schema)

    data_files1 = [DataFile("file1.parquet", 50, 5, {'col_a': {'min': 1, 'max': 5}})]
    time.sleep(0.01)
    table.append_data(data_files1)
    
    new_schema_fields = [{'name': 'col_a', 'type': 'int'}, {'name': 'col_b', 'type': 'float'}]
    time.sleep(0.01)
    table.update_schema(new_schema_fields)

    data_files2 = [DataFile("file2.parquet", 70, 7, {'col_a': {'min': 6, 'max': 12}, 'col_b': {'min': 1.0, 'max': 7.0}})]
    time.sleep(0.01)
    table.append_data(data_files2)

    original_state_json = table.serialize()
    deserialized_table = IcebergTableMetadata.deserialize(original_state_json)

    assert deserialized_table.table_name == table.table_name
    assert deserialized_table._current_snapshot_id == table._current_snapshot_id
    assert len(deserialized_table._schemas) == len(table._schemas)
    assert len(deserialized_table._manifest_files) == len(table._manifest_files)
    assert len(deserialized_table._manifest_lists) == len(table._manifest_lists)
    assert len(deserialized_table._snapshots) == len(table._snapshots)

    # Verify current state after deserialization
    original_current_state = table.get_table_state()
    deserialized_current_state = deserialized_table.get_table_state()
    assert original_current_state == deserialized_current_state

    # Verify history after deserialization
    original_history = table.get_history()
    deserialized_history = deserialized_table.get_history()
    assert original_history == deserialized_history

    # Verify time travel after deserialization
    original_history = table.get_history()
    # Find the snapshot ID for the first append operation
    first_append_snapshot_id = next(s['snapshot_id'] for s in original_history if s['operation'] == 'append')
    original_first_append_state = table.get_table_state(first_append_snapshot_id)
    deserialized_first_append_state = deserialized_table.get_table_state(first_append_snapshot_id)
    assert original_first_append_state == deserialized_first_append_state

def test_get_history():
    initial_schema = [{'name': 'a', 'type': 'int'}]
    table = IcebergTableMetadata("history_table", initial_schema)
    
    # Create snapshot (implicit from init)
    
    # Append 1
    _ = table.append_data([DataFile("f1.parquet", 10, 1, {'a': {'min': 1, 'max': 1}})])
    time.sleep(0.01) # Ensure timestamps are different
    
    # Schema Update
    _ = table.update_schema([{'name': 'a', 'type': 'int'}, {'name': 'b', 'type': 'string'}])
    time.sleep(0.01)
    
    # Append 2
    _ = table.append_data([DataFile("f2.parquet", 20, 2, {'a': {'min': 2, 'max': 3}})])

    history = table.get_history()
    assert len(history) == 4
    assert history[0]['operation'] == 'create'
    assert history[1]['operation'] == 'append'
    assert history[2]['operation'] == 'schema_update'
    assert history[3]['operation'] == 'append'
    
    # Check chronological order by timestamp
    timestamps = [s['timestamp_ms'] for s in history]
    assert all(timestamps[i] <= timestamps[i+1] for i in range(len(timestamps) - 1))

def test_invalid_snapshot_id():
    initial_schema = [{'name': 'a', 'type': 'int'}]
    table = IcebergTableMetadata("invalid_snapshot_table", initial_schema)
    
    with pytest.raises(ValueError, match="Snapshot ID 'non_existent' not found"): 
        table.get_table_state(snapshot_id="non_existent")

    # Test behavior when _current_snapshot_id is somehow None (e.g., corrupted state)
    original_current_snapshot_id = table._current_snapshot_id
    table._current_snapshot_id = None
    with pytest.raises(ValueError, match="Table not initialized or current snapshot is missing."):
        table.get_table_state()
    with pytest.raises(ValueError, match="Table not initialized or current snapshot is missing."):
        table.append_data([DataFile("x.parquet", 1, 1, {})])
    with pytest.raises(ValueError, match="Cannot update schema for an uninitialized table."):
        table.update_schema([{'name': 'x', 'type': 'int'}])
    # Restore for safety, though pytest tears down fixtures
    table._current_snapshot_id = original_current_snapshot_id
