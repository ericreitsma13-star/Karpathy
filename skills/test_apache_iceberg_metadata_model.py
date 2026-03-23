import sys
from skills.apache_iceberg_metadata_model import *

import pytest
import time
import uuid

# Assume the classes IcebergDataFile, IcebergManifestFile, IcebergManifestList,
# IcebergSnapshot, IcebergMetadataFile, IcebergCatalog are available in the scope.
# The `proposed_skill` import is handled automatically by the system.


def test_data_file_creation():
    df = IcebergDataFile(
        path="s3://bucket/data/file.parquet",
        size=1000,
        record_count=50,
        column_stats={"col1": {"min": 0, "max": 100}},
        partition_values={"part": "val1"}
    )
    assert df.path == "s3://bucket/data/file.parquet"
    assert df.size == 1000
    assert df.record_count == 50
    assert df.column_stats["col1"]["min"] == 0
    assert df.file_format == "parquet"
    assert df.to_json_serializable()["path"] == df.path

    with pytest.raises(ValueError):
        IcebergDataFile(path="", size=10, record_count=1)
    with pytest.raises(ValueError):
        IcebergDataFile(path="a.parquet", size=-1, record_count=1)


def test_manifest_file_creation():
    df1 = IcebergDataFile("s3://data/f1.parquet", 100, 10)
    df2 = IcebergDataFile("s3://data/f2.parquet", 200, 20)
    mf = IcebergManifestFile(
        manifest_path="s3://meta/m1.avro",
        data_files=[df1, df2],
        schema_id=0,
        partition_spec_id=0,
        added_snapshot_id=1
    )
    assert mf.manifest_path == "s3://meta/m1.avro"
    assert len(mf.data_files) == 2
    assert mf.data_files[0].path == df1.path
    assert mf.schema_id == 0
    assert mf.to_json_serializable()["manifest_path"] == mf.manifest_path

    with pytest.raises(ValueError):
        IcebergManifestFile(manifest_path="", data_files=[], schema_id=0, partition_spec_id=0, added_snapshot_id=1)
    with pytest.raises(ValueError):
        IcebergManifestFile(manifest_path="m.avro", data_files=[None], schema_id=0, partition_spec_id=0, added_snapshot_id=1)


def test_manifest_list_creation():
    df = IcebergDataFile("s3://data/f1.parquet", 100, 10)
    mf = IcebergManifestFile("s3://meta/m1.avro", [df], 0, 0, 1)
    ml = IcebergManifestList(
        manifest_list_path="s3://meta/ml1.avro",
        manifest_files=[mf],
        snapshot_id=1
    )
    assert ml.manifest_list_path == "s3://meta/ml1.avro"
    assert len(ml.manifest_files) == 1
    assert ml.manifest_files[0].manifest_path == mf.manifest_path
    assert ml.snapshot_id == 1
    assert ml.to_json_serializable()["manifest_list_path"] == ml.manifest_list_path

    with pytest.raises(ValueError):
        IcebergManifestList(manifest_list_path="", manifest_files=[], snapshot_id=1)


def test_snapshot_creation():
    df = IcebergDataFile("s3://data/f1.parquet", 100, 10)
    mf = IcebergManifestFile("s3://meta/m1.avro", [df], 0, 0, 1)
    ml = IcebergManifestList("s3://meta/ml1.avro", [mf], 1)
    
    current_time_ms = int(time.time() * 1000)
    snapshot = IcebergSnapshot(
        snapshot_id=1,
        manifest_list=ml,
        parent_snapshot_id=0,
        operation="append"
    )
    assert snapshot.snapshot_id == 1
    assert snapshot.manifest_list.manifest_list_path == ml.manifest_list_path
    assert snapshot.parent_snapshot_id == 0
    assert snapshot.operation == "append"
    assert snapshot.timestamp_ms >= current_time_ms # Should be current or slightly after
    assert snapshot.to_json_serializable()["snapshot_id"] == snapshot.snapshot_id

    with pytest.raises(ValueError):
        IcebergSnapshot(snapshot_id=1, manifest_list=None)
    with pytest.raises(ValueError):
        IcebergSnapshot(snapshot_id=1, manifest_list=ml, operation="invalid_op")


def test_metadata_file_initialization_and_snapshot_addition():
    table_uuid = str(uuid.uuid4())
    schema = {"fields": [{"name": "id", "type": "int"}]}
    metadata_file = IcebergMetadataFile(
        table_location="s3://my-lake/table/",
        table_uuid=table_uuid,
        current_schema=schema
    )

    assert metadata_file.table_location == "s3://my-lake/table/"
    assert metadata_file.table_uuid == table_uuid
    assert metadata_file.current_schema_id == 0
    assert metadata_file.schemas[0] == schema
    assert metadata_file.current_snapshot_id is None
    assert not metadata_file.snapshots

    df = IcebergDataFile("s3://data/f1.parquet", 100, 10)
    mf = IcebergManifestFile("s3://meta/m1.avro", [df], 0, 0, 0)
    ml = IcebergManifestList("s3://meta/ml0.avro", [mf], 0)

    snapshot0 = metadata_file.add_snapshot(ml)
    assert snapshot0.snapshot_id == 0
    assert metadata_file.current_snapshot_id == 0
    assert metadata_file.get_current_snapshot() == snapshot0
    assert len(metadata_file.snapshots) == 1
    assert metadata_file.snapshot_log[0]["snapshot_id"] == 0

    df2 = IcebergDataFile("s3://data/f2.parquet", 200, 20)
    mf2 = IcebergManifestFile("s3://meta/m2.avro", [df2], 0, 0, 1)
    ml2 = IcebergManifestList("s3://meta/ml1.avro", [mf, mf2], 1)

    snapshot1 = metadata_file.add_snapshot(ml2, operation="append")
    assert snapshot1.snapshot_id == 1
    assert metadata_file.current_snapshot_id == 1
    assert snapshot1.parent_snapshot_id == 0
    assert len(metadata_file.snapshots) == 2
    assert metadata_file.snapshot_log[1]["snapshot_id"] == 1


def test_metadata_file_schema_evolution():
    metadata_file = IcebergMetadataFile(table_location="s3://my-lake/table-schema-evo/")
    
    df = IcebergDataFile("s3://data/f1.parquet", 100, 10)
    mf = IcebergManifestFile("s3://meta/m1.avro", [df], 0, 0, 0)
    ml = IcebergManifestList("s3://meta/ml0.avro", [mf], 0)
    metadata_file.add_snapshot(ml)

    assert metadata_file.current_schema_id == 0
    assert metadata_file.schemas[0] == {"fields": []}

    new_schema = {"fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]}
    df2 = IcebergDataFile("s3://data/f2.parquet", 100, 10)
    mf2 = IcebergManifestFile("s3://meta/m2.avro", [df2], 1, 0, 1) # Note schema_id=1 for new manifest
    ml2 = IcebergManifestList("s3://meta/ml1.avro", [mf2], 1)

    metadata_file.add_snapshot(ml2, new_schema=new_schema)
    assert metadata_file.current_schema_id == 1
    assert metadata_file.schemas[1] == new_schema
    assert len(metadata_file.schemas) == 2


def test_metadata_file_time_travel():
    metadata_file = IcebergMetadataFile(table_location="s3://my-lake/time-travel/")

    df = IcebergDataFile("s3://data/f1.parquet", 100, 10)
    mf = IcebergManifestFile("s3://meta/m1.avro", [df], 0, 0, 0)
    ml = IcebergManifestList("s3://meta/ml0.avro", [mf], 0)
    snapshot0 = metadata_file.add_snapshot(ml)
    time.sleep(0.01) # Ensure distinct timestamps

    df2 = IcebergDataFile("s3://data/f2.parquet", 100, 10)
    mf2 = IcebergManifestFile("s3://meta/m2.avro", [df2], 0, 0, 1)
    ml2 = IcebergManifestList("s3://meta/ml1.avro", [mf2], 1)
    snapshot1 = metadata_file.add_snapshot(ml2)
    time.sleep(0.01)

    df3 = IcebergDataFile("s3://data/f3.parquet", 100, 10)
    mf3 = IcebergManifestFile("s3://meta/m3.avro", [df3], 0, 0, 2)
    ml3 = IcebergManifestList("s3://meta/ml2.avro", [mf3], 2)
    snapshot2 = metadata_file.add_snapshot(ml3)

    # Travel to before first snapshot
    past_snap = metadata_file.time_travel(snapshot0.timestamp_ms - 1)
    assert past_snap is None

    # Travel to exactly first snapshot
    past_snap = metadata_file.time_travel(snapshot0.timestamp_ms)
    assert past_snap.snapshot_id == snapshot0.snapshot_id

    # Travel to between first and second
    past_snap = metadata_file.time_travel(snapshot1.timestamp_ms - 1)
    assert past_snap.snapshot_id == snapshot0.snapshot_id

    # Travel to current (latest)
    past_snap = metadata_file.time_travel(snapshot2.timestamp_ms)
    assert past_snap.snapshot_id == snapshot2.snapshot_id

    past_snap = metadata_file.time_travel(int(time.time() * 1000) + 1000) # Future time
    assert past_snap.snapshot_id == snapshot2.snapshot_id


def test_catalog_operations():
    catalog = IcebergCatalog("my_test_catalog")
    assert catalog.catalog_name == "my_test_catalog"
    assert catalog.list_tables() == []

    table_name = "test_table"
    table_location = "s3://test-bucket/test_table"
    initial_schema = {"fields": [{"name": "col1", "type": "string"}]}
    
    table_metadata = catalog.create_table(table_name, table_location, schema=initial_schema)
    assert table_metadata is not None
    assert catalog.list_tables() == [table_name]
    assert catalog.load_table(table_name).table_location == table_location
    assert catalog.load_table(table_name).schemas[0] == initial_schema

    with pytest.raises(ValueError, match="already exists"):
        catalog.create_table(table_name, "s3://another-location")

    catalog.drop_table(table_name)
    assert catalog.list_tables() == []
    with pytest.raises(ValueError, match="not found"):
        catalog.load_table(table_name)

    # Test dropping non-existent table (should not raise error, due to pass in drop_table)
    catalog.drop_table("non_existent_table")
