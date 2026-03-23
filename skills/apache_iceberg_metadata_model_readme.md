# apache_iceberg_metadata_model

**Source:** [Apache Iceberg: What It Is and Why Everyone’s Talking About It.](https://youtube.com/watch?v=TsmhRZElPvM)
**Added:** 2026-03-23

```markdown
## What this is
This Python code provides a simplified, in-memory implementation of the core Apache Iceberg metadata model. It defines classes representing the hierarchical structure of an Iceberg table: `IcebergDataFile`, `IcebergManifestFile`, `IcebergManifestList`, `IcebergSnapshot`, `IcebergMetadataFile`, and an `IcebergCatalog`.

It's designed to illustrate the key concepts of how Iceberg tracks data files, manages schema and partition evolution, and enables features like time travel, without integrating with actual storage systems or distributed processing engines.

## The problem it solves
Traditional data lake table formats (like raw Parquet/ORC folders with Hive Metastore) often struggle with:
*   **ACID Guarantees:** Lack of atomic commits for writes, leading to inconsistent reads during concurrent operations.
*   **Consistent Views:** Difficult to get a consistent view of the table at a specific point in time.
*   **Schema Evolution:** Complex and error-prone schema changes (adding/renaming/dropping columns) often requiring data rewrites.
*   **Partition Evolution:** Inability to change partitioning strategies without rewriting all data.
*   **Time Travel:** No native mechanism to query past states of the data.
*   **Efficient Query Planning:** Difficulty in pruning data efficiently based on file-level statistics.

This conceptual model demonstrates how Iceberg's layered metadata (data files -> manifests -> manifest lists -> snapshots) addresses these issues by providing a clear, immutable record of table state changes, enabling consistent views, flexible evolution, and optimized data access.

## How to use it (with a short code example)

This example demonstrates creating an Iceberg table, ingesting data (simulated by adding snapshots), and performing schema evolution and time travel.

```python
import uuid
import time
import json

# (Paste the full class definitions for IcebergDataFile, IcebergManifestFile,
# IcebergManifestList, IcebergSnapshot, IcebergMetadataFile, IcebergCatalog here)

class IcebergDataFile:
    # ... (class definition from original code) ...
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
    # ... (class definition from original code) ...
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
    # ... (class definition from original code) ...
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
    # ... (class definition from original code) ...
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
    # ... (class definition from original code) ...
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

        # Update partition spec if provided
        if new_partition_spec:
            new_partition_spec_id = max(self.partition_specs.keys()) + 1
            self.partition_specs[new_partition_spec_id] = new_partition_spec
            self.default_partition_spec_id = new_partition_spec_id


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
    # ... (class definition from original code) ...
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
        return metadata_file

    def drop_table(self, table_name: str) -> None:
        """Removes a table from the catalog."""
        if table_name in self._tables:
            del self._tables[table_name]
        else:
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

# --- Example Usage ---
catalog = IcebergCatalog()

# 1. Create a table with an initial schema and partition spec
initial_schema = {
    "schema_id": 0,
    "fields": [
        {"id": 1, "name": "timestamp", "type": "long", "required": True},
        {"id": 2, "name": "device_id", "type": "string", "required": True},
        {"id": 3, "name": "temperature", "type": "float", "required": False},
    ]
}
partition_spec = {"spec_id": 0, "fields": [{"field_id": 1000, "source_id": 1, "transform": "day", "name": "dt"}]}
thermostats_table = catalog.create_table(
    "thermostats", "s3://my-data-lake/thermostats",
    schema=initial_schema,
    partition_spec=partition_spec
)
print("Table 'thermostats' created.")

# 2. Simulate First Ingest (creates data files, manifest, manifest list, and a snapshot)
data_file_1 = IcebergDataFile(
    path="s3://my-data-lake/thermostats/data/2023/01/01/data_001.parquet",
    size=1024, record_count=100, column_stats={"temperature": {"min": 20.0}}, partition_values={"dt": "2023-01-01"}
)
manifest_file_1 = IcebergManifestFile(
    manifest_path="s3://my-data-lake/thermostats/metadata/manifest-1.avro",
    data_files=[data_file_1], schema_id=0, partition_spec_id=0, added_snapshot_id=0
)
manifest_list_0 = IcebergManifestList(
    manifest_list_path="s3://my-data-lake/thermostats/metadata/snap-0-manifest-list.avro",
    manifest_files=[manifest_file_1], snapshot_id=0
)
snapshot_0 = thermostats_table.add_snapshot(manifest_list_0, operation="append")
print(f"\nSnapshot {snapshot_0.snapshot_id} (first ingest) added at {snapshot_0.timestamp_ms}.")

# 3. Simulate Second Ingest (new data files, new manifest file, updated manifest list, new snapshot)
time.sleep(0.01) # Ensure distinct timestamp
data_file_2 = IcebergDataFile(
    path="s3://my-data-lake/thermostats/data/2023/01/02/data_002.parquet",
    size=2048, record_count=200, column_stats={"temperature": {"min": 21.0}}, partition_values={"dt": "2023-01-02"}
)
manifest_file_2 = IcebergManifestFile(
    manifest_path="s3://my-data-lake/thermostats/metadata/manifest-2.avro",
    data_files=[data_file_2], schema_id=0, partition_spec_id=0, added_snapshot_id=1
)
# Note: In a real Iceberg, manifest_list_1 would combine manifest_file_1 AND manifest_file_2 for an append
# For simplicity here, we'll create a new one pointing to both existing manifests.
manifest_list_1 = IcebergManifestList(
    manifest_list_path="s3://my-data-lake/thermostats/metadata/snap-1-manifest-list.avro",
    manifest_files=[manifest_file_1, manifest_file_2], snapshot_id=1 # Combines previous and new manifests
)
snapshot_1 = thermostats_table.add_snapshot(manifest_list_1, operation="append")
print(f"Snapshot {snapshot_1.snapshot_id} (second ingest) added at {snapshot_1.timestamp_ms}.")

# 4. Simulate Schema Evolution
time.sleep(0.01) # Ensure distinct timestamp
evolved_schema = {
    "schema_id": 1,
    "fields": initial_schema["fields"] + [{"id": 4, "name": "humidity", "type": "float", "required": False}]
}
manifest_list_2 = IcebergManifestList(
    manifest_list_path="s3://my-data-lake/thermostats/metadata/snap-2-manifest-list.avro",
    manifest_files=[manifest_file_1, manifest_file_2], # The data files haven't changed, only schema
    snapshot_id=2
)
snapshot_2 = thermostats_table.add_snapshot(manifest_list_2, operation="schema_evolution", new_schema=evolved_schema)
print(f"Snapshot {snapshot_2.snapshot_id} (schema evolution) added at {snapshot_2.timestamp_ms}.")
print(f"Current schema (ID {thermostats_table.current_schema_id}): {json.dumps(thermostats_table.schemas[thermostats_table.current_schema_id], indent=2)}")

# 5. Demonstrate Time Travel
print("\n--- Time Travel Simulation ---")
travel_time_after_first_ingest = snapshot_0.timestamp_ms + 1
past_snapshot = thermostats_table.time_travel(travel_time_after_first_ingest)
if past_snapshot:
    print(f"Time travel to {travel_time_after_first_ingest}: Found snapshot ID {past_snapshot.snapshot_id}")
    print(f"  Manifest list path: {past_snapshot.manifest_list.manifest_list_path}")
    # In a real system, you'd then load this manifest list to get data files

travel_time_before_schema_evol = snapshot_1.timestamp_ms + 1
past_snapshot_before_schema = thermostats_table.time_travel(travel_time_before_schema_evol)
if past_snapshot_before_schema:
    print(f"Time travel to {travel_time_before_schema_evol} (before schema evol): Found snapshot ID {past_snapshot_before_schema.snapshot_id}")
    # Current schema at this point would be initial_schema (ID 0)
    print(f"  Manifest list path: {past_snapshot_before_schema.manifest_list.manifest_list_path}")

print("\n--- Full Table Metadata (Current State) ---")
print(json.dumps(thermostats_table.to_json_serializable(), indent=2))
```

## What real-world tool this relates to
This skill directly relates to **Apache Iceberg**, a high-performance open table format for huge analytic datasets. It provides the foundational understanding for how Iceberg manages table state and data files, which powers:
*   **Data Lakehouses:** Providing database-like capabilities (ACID, schema evolution, time travel) on object storage (S3, ADLS, GCS).
*   **Query Engines:** Used by Spark, Flink, Trino, Presto, Dremio, and even cloud data warehouses like Google BigQuery (via BigLake) and AWS Athena/EMR.
*   **Data Ingestion and Transformation:** Enabling reliable and efficient data pipelines.

## Limitations
This is a conceptual model designed for educational purposes, not a production-ready Iceberg implementation. Key limitations include:
*   **No actual data storage/retrieval:** Data files, manifest files, and manifest lists are represented by their paths and in-memory objects, but no actual file I/O to S3/HDFS/etc. occurs.
*   **No distributed processing:** Operations are purely in-memory Python object manipulations, not distributed operations like Spark or Flink would perform.
*   **Simplified concurrency:** Lacks real-world locking mechanisms and conflict resolution for concurrent writes.
*   **Incomplete spec:** Omits many details of the full Iceberg specification (e.g., sort orders, delete files, other manifest entry states, richer schema evolution rules, table properties, branch/tag references).
*   **Performance:** Not optimized for large-scale operations; intended for understanding logical flow.
*   **Catalog Integration:** The `IcebergCatalog` is an in-memory dictionary, not a persistent catalog like Hive Metastore, AWS Glue, or Project Nessie.