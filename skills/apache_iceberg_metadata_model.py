"""
Skill: apache_iceberg_metadata_model
Source: https://youtube.com/watch?v=TsmhRZElPvM
Title: Apache Iceberg: What It Is and Why Everyone’s Talking About It.
Added: 2026-03-23
"""

import uuid
import time
import json

class IcebergDataFile:
    """
    Represents a data file (e.g., Parquet) as described in Iceberg's data layer.
    Includes metadata relevant for query optimization.
    """
    def __init__(self, path: str, size: int, record_count: int,
                 column_stats: dict = None, partition_values: dict = None):
        if not path or not isinstance(path, str):
            raise ValueError("Data file path must be a non-empty string.")
        if not isinstance(size, int) or size < 0:
            raise ValueError("Data file size must be a non-negative integer.")
        if not isinstance(record_count, int) or record_count < 0:
            raise ValueError("Data file record count must be a non-negative integer.")

        self.path = path
        self.size = size  # in bytes
        self.record_count = record_count
        self.column_stats = column_stats if column_stats is not None else {}
        self.partition_values = partition_values if partition_values is not None else {}
        self.file_format = path.split('.')[-1] # Infer format from path, e.g., 'parquet'

    def to_json_serializable(self):
        """Returns a dictionary representation suitable for JSON serialization."""
        return {
            "path": self.path,
            "size": self.size,
            "record_count": self.record_count,
            "column_stats": self.column_stats,
            "partition_values": self.partition_values,
            "file_format": self.file_format
        }

class IcebergManifestFile:
    """
    Represents an Iceberg manifest file, which lists data files
    and their associated metadata (like schema ID, partition spec ID).
    Conceptually, this is also a file in the blob store.
    """
    def __init__(self, manifest_path: str, data_files: list, # Changed type hint to avoid direct dependency on IcebergDataFile for lists
                 schema_id: int, partition_spec_id: int, added_snapshot_id: int):
        if not manifest_path or not isinstance(manifest_path, str):
            raise ValueError("Manifest file path must be a non-empty string.")
        if not isinstance(data_files, list) or not all(isinstance(f, IcebergDataFile) for f in data_files):
            raise ValueError("Data files must be a list of IcebergDataFile instances.")
        if not isinstance(schema_id, int) or schema_id < 0:
            raise ValueError("Schema ID must be a non-negative integer.")
        if not isinstance(partition_spec_id, int) or partition_spec_id < 0:
            raise ValueError("Partition spec ID must be a non-negative integer.")
        if not isinstance(added_snapshot_id, int):
            raise ValueError("Added snapshot ID must be an integer.")

        self.manifest_path = manifest_path
        self.data_files = data_files
        self.schema_id = schema_id
        self.partition_spec_id = partition_spec_id
        self.added_snapshot_id = added_snapshot_id
        self.content_type = "data_file_entry" # As per Iceberg spec for manifest entries

    def to_json_serializable(self):
        """Returns a dictionary representation suitable for JSON serialization."""
        return {
            "manifest_path": self.manifest_path,
            "data_file_entries": [df.to_json_serializable() for df in self.data_files],
            "schema_id": self.schema_id,
            "partition_spec_id": self.partition_spec_id,
            "added_snapshot_id": self.added_snapshot_id,
            "content_type": self.content_type
        }

class IcebergManifestList:
    """
    Represents an Iceberg manifest list file, which points to multiple
    manifest files, allowing collection of data from multiple ingest events.
    Conceptually, this is also a file in the blob store.
    """
    def __init__(self, manifest_list_path: str, manifest_files: list, # Changed type hint
                 snapshot_id: int):
        if not manifest_list_path or not isinstance(manifest_list_path, str):
            raise ValueError("Manifest list path must be a non-empty string.")
        if not isinstance(manifest_files, list) or not all(isinstance(mf, IcebergManifestFile) for mf in manifest_files):
            raise ValueError("Manifest files must be a list of IcebergManifestFile instances.")
        if not isinstance(snapshot_id, int):
            raise ValueError("Snapshot ID must be an integer.")

        self.manifest_list_path = manifest_list_path
        self.manifest_files = manifest_files
        self.snapshot_id = snapshot_id # The snapshot this manifest list belongs to

    def to_json_serializable(self):
        """Returns a dictionary representation suitable for JSON serialization."""
        return {
            "manifest_list_path": self.manifest_list_path,
            "manifest_file_entries": [mf.to_json_serializable() for mf in self.manifest_files],
            "snapshot_id": self.snapshot_id
        }

class IcebergSnapshot:
    """
    Represents a single snapshot of an Iceberg table, pointing to a ManifestList.
    This enables consistent views of the table.
    """
    def __init__(self, snapshot_id: int, manifest_list: IcebergManifestList,
                 timestamp_ms: int = None, parent_snapshot_id: int = None,
                 operation: str = "append"):
        if not isinstance(snapshot_id, int):
            raise ValueError("Snapshot ID must be an integer.")
        if not isinstance(manifest_list, IcebergManifestList):
            raise ValueError("Manifest list must be an IcebergManifestList instance.")
        if parent_snapshot_id is not None and not isinstance(parent_snapshot_id, int):
            raise ValueError("Parent snapshot ID must be an integer or None.")
        if operation not in ["append", "overwrite", "delete", "replace", "rollback"]:
            raise ValueError(f"Invalid operation '{operation}'.")

        self.snapshot_id = snapshot_id
        self.manifest_list = manifest_list
        self.timestamp_ms = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
        self.parent_snapshot_id = parent_snapshot_id
        self.operation = operation

    def to_json_serializable(self):
        """Returns a dictionary representation suitable for JSON serialization."""
        return {
            "snapshot_id": self.snapshot_id,
            "timestamp_ms": self.timestamp_ms,
            "manifest_list_path": self.manifest_list.manifest_list_path, # Only store path for metadata file
            "parent_snapshot_id": self.parent_snapshot_id,
            "operation": self.operation
        }

class IcebergMetadataFile:
    """
    The central metadata file for an Iceberg table, containing current schema,
    partition specs, and a history of snapshots. This is what the catalog points to.
    """
    def __init__(self, table_location: str, table_uuid: str = None,
                 current_schema: dict = None, partition_specs: list = None):
        if not table_location or not isinstance(table_location, str):
            raise ValueError("Table location must be a non-empty string.")

        self.table_location = table_location # Base path for the table
        self.table_uuid = table_uuid if table_uuid is not None else str(uuid.uuid4())
        self.format_version = 2 # Current default Iceberg format version
        self.last_sequence_number = 0
        self.last_updated_ms = int(time.time() * 1000)
        self.schemas = {0: current_schema} if current_schema is not None else {0: {"fields": []}}
        self.current_schema_id = 0
        self.partition_specs = {0: partition_specs} if partition_specs is not None else {0: []}
        self.default_partition_spec_id = 0
        self.properties = {} # Table properties
        self.snapshots: dict = {} # Map snapshot_id to IcebergSnapshot object
        self.current_snapshot_id: int | None = None
        self.snapshot_log = [] # A chronological list of snapshot IDs

    def _generate_snapshot_id(self) -> int:
        return max(self.snapshots.keys(), default=-1) + 1

    def add_snapshot(self, manifest_list: IcebergManifestList, operation: str = "append",
                     new_schema: dict = None, new_partition_spec: dict = None) -> IcebergSnapshot:
        """
        Adds a new snapshot to the table metadata, representing a change.
        Handles schema and partition spec evolution.
        """
        snapshot_id = self._generate_snapshot_id()
        parent_snapshot_id = self.current_snapshot_id

        # Update schema if provided
        if new_schema:
            new_schema_id = max(self.schemas.keys()) + 1
            self.schemas[new_schema_id] = new_schema
            self.current_schema_id = new_schema_id
            # print(f"Schema evolved to ID: {new_schema_id}") # Commented out print for clean output

        # Update partition spec if provided
        if new_partition_spec:
            new_partition_spec_id = max(self.partition_specs.keys()) + 1
            self.partition_specs[new_partition_spec_id] = new_partition_spec
            self.default_partition_spec_id = new_partition_spec_id
            # print(f"Partition spec evolved to ID: {new_partition_spec_id}") # Commented out print for clean output


        snapshot = IcebergSnapshot(
            snapshot_id=snapshot_id,
            manifest_list=manifest_list,
            parent_snapshot_id=parent_snapshot_id,
            operation=operation
        )
        self.snapshots[snapshot_id] = snapshot
        self.current_snapshot_id = snapshot_id
        self.snapshot_log.append({"snapshot_id": snapshot_id, "timestamp_ms": snapshot.timestamp_ms})
        self.last_updated_ms = int(time.time() * 1000)
        # print(f"Added snapshot {snapshot_id} for table {self.table_location}") # Commented out print for clean output
        return snapshot

    def get_current_snapshot(self) -> IcebergSnapshot | None:
        """Returns the current active snapshot."""
        if self.current_snapshot_id is not None:
            return self.snapshots.get(self.current_snapshot_id)
        return None

    def time_travel(self, timestamp_ms: int) -> IcebergSnapshot | None:
        """
        Simulates time travel to find the latest snapshot before or at a given timestamp.
        """
        for entry in reversed(self.snapshot_log):
            if entry["timestamp_ms"] <= timestamp_ms:
                return self.snapshots.get(entry["snapshot_id"])
        return None

    def to_json_serializable(self):
        """Returns a dictionary representation suitable for JSON serialization."""
        # For metadata file, manifest lists and data files are referenced by path, not full objects
        return {
            "format_version": self.format_version,
            "table_uuid": self.table_uuid,
            "location": self.table_location,
            "last_sequence_number": self.last_sequence_number,
            "last_updated_ms": self.last_updated_ms,
            "schemas": self.schemas,
            "current_schema_id": self.current_schema_id,
            "partition_specs": self.partition_specs,
            "default_partition_spec_id": self.default_partition_spec_id,
            "properties": self.properties,
            "current_snapshot_id": self.current_snapshot_id,
            "snapshots": {
                sid: snap.to_json_serializable() for sid, snap in self.snapshots.items()
            },
            "snapshot_log": self.snapshot_log
            # Placeholder for sort orders, refs, etc.
        }


class IcebergCatalog:
    """
    A conceptual Iceberg Catalog that maps table names to their MetadataFile locations.
    Can be pluggable (Hive Metastore, JDBC, in-memory for this example).
    """
    def __init__(self, catalog_name: str = "default_catalog"):
        self.catalog_name = catalog_name
        self._tables: dict = {} # In-memory storage

    def create_table(self, table_name: str, table_location: str,
                     schema: dict = None, partition_spec: dict = None) -> IcebergMetadataFile:
        """Creates a new Iceberg table entry in the catalog."""
        if table_name in self._tables:
            raise ValueError(f"Table '{table_name}' already exists.")

        metadata_file = IcebergMetadataFile(
            table_location=table_location,
            current_schema=schema,
            partition_specs=[partition_spec] if partition_spec else []
        )
        self._tables[table_name] = metadata_file
        # print(f"Table '{table_name}' created at '{table_location}'.") # Commented out print for clean output
        return metadata_file

    def drop_table(self, table_name: str) -> None:
        """Removes a table from the catalog."""
        if table_name in self._tables:
            del self._tables[table_name]
            # print(f"Table '{table_name}' dropped.") # Commented out print for clean output
        else:
            # print(f"Table '{table_name}' not found.") # Commented out print for clean output
            pass # No error for non-existent drop, mimicking some systems

    def load_table(self, table_name: str) -> IcebergMetadataFile:
        """Loads an Iceberg table's metadata from the catalog."""
        if table_name not in self._tables:
            raise ValueError(f"Table '{table_name}' not found in catalog.")
        return self._tables[table_name]

    def list_tables(self) -> list[str]:
        """Lists all table names in the catalog."""
        return list(self._tables.keys())

    def to_json_serializable(self):
        """Returns a dictionary representation of the catalog's state."""
        return {
            "catalog_name": self.catalog_name,
            "tables": {
                name: table.to_json_serializable() for name, table in self._tables.items()
            }
        }

# The example usage below is for demonstration and not part of the reusable module.
# It's commented out to ensure the module is self-contained without direct execution logic.
# def simulate_iceberg_workflow():
#     catalog = IcebergCatalog()

#     # 1. Create a table
#     initial_schema = {
#         "schema_id": 0,
#         "fields": [
#             {"id": 1, "name": "timestamp", "type": "long", "required": True},
#             {"id": 2, "name": "device_id", "type": "string", "required": True},
#             {"id": 3, "name": "temperature", "type": "float", "required": False},
#         ]
#     }
#     thermostats_table_metadata = catalog.create_table(
#         "thermostats", "s3://my-data-lake/thermostats",
#         schema=initial_schema,
#         partition_spec={"spec_id": 0, "fields": [{"field_id": 1000, "source_id": 1, "transform": "day", "name": "dt"}]}
#     )

#     # 2. First Ingest (creates data files, manifest file, manifest list, and a snapshot)
#     data_file_1 = IcebergDataFile(
#         path="s3://my-data-lake/thermostats/data/2023/01/01/data_001.parquet",
#         size=1024, record_count=100,
#         column_stats={"temperature": {"min": 20.0, "max": 25.5}},
#         partition_values={"dt": "2023-01-01"}
#     )
#     data_file_2 = IcebergDataFile(
#         path="s3://my-data-lake/thermostats/data/2023/01/01/data_002.parquet",
#         size=2048, record_count=200,
#         column_stats={"temperature": {"min": 21.0, "max": 26.0}},
#         partition_values={"dt": "2023-01-01"}
#     )

#     manifest_file_1 = IcebergManifestFile(
#         manifest_path="s3://my-data-lake/thermostats/metadata/manifest-1.avro",
#         data_files=[data_file_1, data_file_2],
#         schema_id=0,
#         partition_spec_id=0,
#         added_snapshot_id=0
#     )

#     manifest_list_0 = IcebergManifestList(
#         manifest_list_path="s3://my-data-lake/thermostats/metadata/snap-0-manifest-list.avro",
#         manifest_files=[manifest_file_1],
#         snapshot_id=0
#     )

#     thermostats_table_metadata.add_snapshot(manifest_list_0, operation="append")
#     # print("\n--- After First Ingest ---")
#     # print(json.dumps(thermostats_table_metadata.get_current_snapshot().to_json_serializable(), indent=2))
#     # print(f"Current table schema ID: {thermostats_table_metadata.current_schema_id}")

#     # 3. Second Ingest (new data files, new manifest file, updated manifest list, new snapshot)
#     data_file_3 = IcebergDataFile(
#         path="s3://my-data-lake/thermostats/data/2023/01/02/data_003.parquet",
#         size=1536, record_count=150,
#         column_stats={"temperature": {"min": 22.0, "max": 27.0}},
#         partition_values={"dt": "2023-01-02"}
#     )

#     manifest_file_2 = IcebergManifestFile(
#         manifest_path="s3://my-data-lake/thermostats/metadata/manifest-2.avro",
#         data_files=[data_file_3],
#         schema_id=0,
#         partition_spec_id=0,
#         added_snapshot_id=1 # This manifest was added in snapshot 1
#     )

#     # A new manifest list combining previous manifest file and the new one
#     manifest_list_1 = IcebergManifestList(
#         manifest_list_path="s3://my-data-lake/thermostats/metadata/snap-1-manifest-list.avro",
#         manifest_files=[manifest_file_1, manifest_file_2], # Old manifest + new manifest
#         snapshot_id=1
#     )
#     thermostats_table_metadata.add_snapshot(manifest_list_1, operation="append")
#     # print("\n--- After Second Ingest ---")
#     # current_snap = thermostats_table_metadata.get_current_snapshot()
#     # print(json.dumps(current_snap.to_json_serializable(), indent=2))
#     # print(f"Snapshot parent ID: {current_snap.parent_snapshot_id}")

#     # 4. Schema Evolution
#     evolved_schema = {
#         "schema_id": 1,
#         "fields": [
#             {"id": 1, "name": "timestamp", "type": "long", "required": True},
#             {"id": 2, "name": "device_id", "type": "string", "required": True},
#             {"id": 3, "name": "temperature", "type": "float", "required": False},
#             {"id": 4, "name": "humidity", "type": "float", "required": False}, # New column
#         ]
#     }
#     # For schema evolution, typically a new snapshot is created that refers to the new schema
#     # but the manifest list might still refer to old data files (which will have nulls for new column)
#     # or new manifest lists point to new data files with updated schema.
#     # Here we simulate by just updating the schema in a new snapshot.
#     manifest_list_2 = IcebergManifestList(
#         manifest_list_path="s3://my-data-lake/thermostats/metadata/snap-2-manifest-list.avro",
#         manifest_files=[manifest_file_1, manifest_file_2], # For simplicity, re-using previous manifest files
#         snapshot_id=2
#     )
#     thermostats_table_metadata.add_snapshot(manifest_list_2, operation="overwrite", new_schema=evolved_schema)
#     # print("\n--- After Schema Evolution ---")
#     # print(json.dumps(thermostats_table_metadata.get_current_snapshot().to_json_serializable(), indent=2))
#     # print(f"Current table schema ID: {thermostats_table_metadata.current_schema_id}")
#     # print(json.dumps(thermostats_table_metadata.schemas[thermostats_table_metadata.current_schema_id], indent=2))

#     # 5. Time Travel
#     first_snapshot_time = thermostats_table_metadata.snapshots[0].timestamp_ms
#     travel_time = first_snapshot_time + 10 # Just after the first snapshot
#     past_snapshot = thermostats_table_metadata.time_travel(travel_time)
#     # print(f"\n--- Time Travel to {travel_time} ---")
#     # if past_snapshot:
#     #     print(f"Found snapshot ID: {past_snapshot.snapshot_id}")
#     # else:
#     #     print("No snapshot found for the given time.")

#     # print("\n--- Catalog State ---")
#     # print(json.dumps(catalog.to_json_serializable(), indent=2))

# if __name__ == "__main__":
#     simulate_iceberg_workflow()
