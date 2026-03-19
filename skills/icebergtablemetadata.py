"""
Skill: IcebergTableMetadata
Source: https://youtube.com/watch?v=TsmhRZElPvM
Title: Apache Iceberg: What It Is and Why Everyone’s Talking About It.
Added: 2026-03-19
"""

import json
import uuid
import time
from typing import List, Dict, Any, Optional

# --- Data File Representation ---
class DataFile:
    """Represents a data file (e.g., Parquet) with basic statistics."""
    def __init__(self, path: str, size: int, row_count: int, column_stats: Dict[str, Dict[str, Any]]):
        self.path = path
        self.size = size
        self.row_count = row_count
        self.column_stats = column_stats # e.g., {'col1': {'min': 0, 'max': 100, 'null_count': 5}}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "size": self.size,
            "row_count": self.row_count,
            "column_stats": self.column_stats
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataFile':
        return cls(data["path"], data["size"], data["row_count"], data["column_stats"])

# --- Manifest File Representation ---
class ManifestFile:
    """Lists data files and includes partition/schema metadata and aggregated stats."""
    def __init__(self, manifest_id: str, data_files: List['DataFile'], schema_id: str):
        self.manifest_id = manifest_id
        self.data_files = data_files
        self.schema_id = schema_id
        self._calculate_stats_summary()

    def _calculate_stats_summary(self):
        # In a real system, this would aggregate stats from data_files more comprehensively.
        # For simplicity, we just sum row counts here.
        self.total_row_count = sum(df.row_count for df in self.data_files)
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "manifest_id": self.manifest_id,
            "data_files": [df.to_dict() for df in self.data_files],
            "schema_id": self.schema_id,
            "total_row_count": self.total_row_count
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ManifestFile':
        manifest = cls(data["manifest_id"], [DataFile.from_dict(df_data) for df_data in data["data_files"]], data["schema_id"])
        manifest.total_row_count = data.get("total_row_count", 0) # For backward compatibility in case of older serialized data
        return manifest

# --- Manifest List Representation ---
class ManifestList:
    """Lists manifest files, forming a logical collection of data files for a table version."""
    def __init__(self, list_id: str, manifest_files: List['ManifestFile']):
        self.list_id = list_id
        self.manifest_files = manifest_files

    def to_dict(self) -> Dict[str, Any]:
        return {
            "list_id": self.list_id,
            "manifest_files": [mf.to_dict() for mf in self.manifest_files]
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ManifestList':
        return cls(data["list_id"], [ManifestFile.from_dict(mf_data) for mf_data in data["manifest_files"]])

# --- Snapshot Representation ---
class Snapshot:
    """Represents a consistent, atomic version of the table, pointing to a ManifestList."""
    def __init__(self, snapshot_id: str, manifest_list_id: str, timestamp_ms: int, operation: str, schema_id: str):
        self.snapshot_id = snapshot_id
        self.manifest_list_id = manifest_list_id
        self.timestamp_ms = timestamp_ms
        self.operation = operation
        self.schema_id = schema_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "manifest_list_id": self.manifest_list_id,
            "timestamp_ms": self.timestamp_ms,
            "operation": self.operation,
            "schema_id": self.schema_id
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Snapshot':
        return cls(data["snapshot_id"], data["manifest_list_id"], data["timestamp_ms"], data["operation"], data["schema_id"])

# --- Table Schema Representation ---
class TableSchema:
    """Defines the schema of the table at a specific point in time."""
    def __init__(self, schema_id: str, fields: List[Dict[str, Any]]):
        self.schema_id = schema_id
        self.fields = fields # e.g., [{'name': 'id', 'type': 'int'}, {'name': 'name', 'type': 'string'}]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "fields": self.fields
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableSchema':
        return cls(data["schema_id"], data["fields"])

# --- Iceberg-like Table Metadata Manager ---
class IcebergTableMetadata:
    """
    A simplified in-memory representation of Apache Iceberg table metadata.
    This class simulates the hierarchical structure of snapshots, manifest lists,
    manifest files, and data files, along with schema evolution and transactional updates.
    It demonstrates the core principles of an open table format.
    """
    def __init__(self, table_name: str, initial_schema: List[Dict[str, Any]]):
        self.table_name = table_name
        self._schemas: Dict[str, TableSchema] = {}
        self._manifest_files: Dict[str, ManifestFile] = {}
        self._manifest_lists: Dict[str, ManifestList] = {}
        self._snapshots: Dict[str, Snapshot] = {}
        self._current_snapshot_id: Optional[str] = None
        
        # Initialize with a base schema
        initial_schema_obj = TableSchema(str(uuid.uuid4()), initial_schema)
        self._schemas[initial_schema_obj.schema_id] = initial_schema_obj

        # Create an empty initial state, mimicking the first table version
        empty_manifest_file = ManifestFile(str(uuid.uuid4()), [], initial_schema_obj.schema_id)
        self._manifest_files[empty_manifest_file.manifest_id] = empty_manifest_file

        empty_manifest_list = ManifestList(str(uuid.uuid4()), [empty_manifest_file])
        self._manifest_lists[empty_manifest_list.list_id] = empty_manifest_list

        initial_snapshot = Snapshot(
            str(uuid.uuid4()),
            empty_manifest_list.list_id,
            int(time.time() * 1000),
            "create",
            initial_schema_obj.schema_id
        )
        self._snapshots[initial_snapshot.snapshot_id] = initial_snapshot
        self._current_snapshot_id = initial_snapshot.snapshot_id

    def _get_current_snapshot(self) -> Snapshot:
        if self._current_snapshot_id is None: # Use explicit 'is None' for clarity
            raise ValueError("Table not initialized or current snapshot is missing.")
        return self._snapshots[self._current_snapshot_id]

    def _create_new_manifest_list(self, previous_manifest_list: ManifestList, new_manifest_file: ManifestFile) -> ManifestList:
        """Creates a new ManifestList by combining existing and new manifest files."""
        new_manifest_files = previous_manifest_list.manifest_files + [new_manifest_file]
        new_list = ManifestList(str(uuid.uuid4()), new_manifest_files)
        self._manifest_lists[new_list.list_id] = new_list
        return new_list

    def _create_new_snapshot(self, manifest_list_id: str, operation: str, schema_id: str) -> Snapshot:
        """Creates a new Snapshot and sets it as the current table state."""
        new_snapshot = Snapshot(
            str(uuid.uuid4()),
            manifest_list_id,
            int(time.time() * 1000),
            operation,
            schema_id
        )
        self._snapshots[new_snapshot.snapshot_id] = new_snapshot
        self._current_snapshot_id = new_snapshot.snapshot_id
        return new_snapshot

    def append_data(self, data_files: List[DataFile]) -> str:
        """
        Appends new data files to the table.
        This operation is 'transactional' in that it creates new metadata versions
        (new manifest file, new manifest list, new snapshot) without modifying
        previous states.
        """
        if self._current_snapshot_id is None: # Changed for consistency with test expectation
            raise ValueError("Table not initialized or current snapshot is missing.")

        current_snapshot = self._get_current_snapshot()
        current_schema_id = current_snapshot.schema_id

        # 1. Create a new ManifestFile for the incoming data
        new_manifest_file = ManifestFile(str(uuid.uuid4()), data_files, current_schema_id)
        self._manifest_files[new_manifest_file.manifest_id] = new_manifest_file

        # 2. Create a new ManifestList that references all previous manifest files
        #    plus the newly created manifest file.
        previous_manifest_list = self._manifest_lists[current_snapshot.manifest_list_id]
        new_manifest_list = self._create_new_manifest_list(previous_manifest_list, new_manifest_file)

        # 3. Create a new Snapshot pointing to the new ManifestList, marking the new table state.
        new_snapshot = self._create_new_snapshot(new_manifest_list.list_id, "append", current_schema_id)
        
        return new_snapshot.snapshot_id

    def update_schema(self, new_schema_fields: List[Dict[str, Any]]) -> str:
        """
        Updates the table schema. This also creates a new snapshot.
        Existing data files still use their original schemas, but new reads will interpret
        them based on the latest schema (schema evolution).
        """
        if self._current_snapshot_id is None: # Use explicit 'is None' for clarity
            raise ValueError("Cannot update schema for an uninitialized table.")

        # 1. Create a new schema object with a new ID
        new_schema = TableSchema(str(uuid.uuid4()), new_schema_fields)
        self._schemas[new_schema.schema_id] = new_schema

        # 2. Create a new snapshot that points to the *current* ManifestList but
        #    references the *new* schema. This allows readers using the new snapshot
        #    to apply the evolved schema.
        current_snapshot = self._get_current_snapshot()
        new_snapshot = self._create_new_snapshot(current_snapshot.manifest_list_id, "schema_update", new_schema.schema_id)
        return new_snapshot.snapshot_id

    def get_table_state(self, snapshot_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieves the table's state at a given snapshot (for time travel) or the current state.
        """
        target_snapshot_id: str
        if snapshot_id is None: # Requesting current state
            if self._current_snapshot_id is None:
                raise ValueError("Table not initialized or current snapshot is missing.")
            target_snapshot_id = self._current_snapshot_id
        else: # Requesting a specific historical snapshot
            target_snapshot_id = snapshot_id
        
        if target_snapshot_id not in self._snapshots:
            # Modified error message to be more specific when ID is not found
            raise ValueError(f"Snapshot ID '{target_snapshot_id}' not found.")

        snapshot = self._snapshots[target_snapshot_id]
        manifest_list = self._manifest_lists[snapshot.manifest_list_id]
        schema = self._schemas[snapshot.schema_id]
        
        all_data_files: List[DataFile] = []
        for manifest_file_in_list in manifest_list.manifest_files:
            # Retrieve the full manifest file object from our in-memory store
            full_manifest_file = self._manifest_files[manifest_file_in_list.manifest_id]
            all_data_files.extend(full_manifest_file.data_files)

        return {
            "table_name": self.table_name,
            "current_snapshot_id": snapshot.snapshot_id,
            "timestamp": snapshot.timestamp_ms,
            "operation": snapshot.operation,
            "schema": schema.to_dict(),
            "data_files": [df.to_dict() for df in all_data_files],
            "total_rows": sum(df.row_count for df in all_data_files)
        }

    def get_history(self) -> List[Dict[str, Any]]:
        """Returns a list of all snapshots, ordered chronologically by timestamp."""
        sorted_snapshots = sorted(self._snapshots.values(), key=lambda s: s.timestamp_ms)
        return [s.to_dict() for s in sorted_snapshots]

    def serialize(self) -> str:
        """Serializes the entire metadata state to a JSON string. Simulates saving to a catalog."""
        state = {
            "table_name": self.table_name,
            "schemas": {sid: s.to_dict() for sid, s in self._schemas.items()},
            "manifest_files": {mfid: mf.to_dict() for mfid, mf in self._manifest_files.items()},
            "manifest_lists": {mlid: ml.to_dict() for mlid, ml in self._manifest_lists.items()},
            "snapshots": {sid: s.to_dict() for sid, s in self._snapshots.items()},
            "current_snapshot_id": self._current_snapshot_id
        }
        return json.dumps(state, indent=2)

    @classmethod
    def deserialize(cls, json_str: str) -> 'IcebergTableMetadata':
        """Deserializes metadata state from a JSON string, reconstructing the table."""
        state = json.loads(json_str)
        
        # We need a dummy initial schema for the __init__ call, it will be overwritten
        dummy_schema = [{'name': 'dummy', 'type': 'string'}] 
        instance = cls(state["table_name"], dummy_schema)
        
        instance._schemas = {sid: TableSchema.from_dict(s_data) for sid, s_data in state["schemas"].items()}
        
        instance._manifest_files = {}
        for mfid, mf_data in state["manifest_files"].items():
            instance._manifest_files[mfid] = ManifestFile.from_dict(mf_data)
            
        instance._manifest_lists = {mlid: ManifestList.from_dict(ml_data) for mlid, ml_data in state["manifest_lists"].items()}
        instance._snapshots = {sid: Snapshot.from_dict(s_data) for sid, s_data in state["snapshots"].items()}
        instance._current_snapshot_id = state["current_snapshot_id"]

        return instance
