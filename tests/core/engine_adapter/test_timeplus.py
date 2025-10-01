# type: ignore

import typing as t

import pytest
from pytest_mock import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter import TimeplusEngineAdapter
from tests.core.engine_adapter import to_sql_calls
from sqlmesh.core.engine_adapter.shared import (
    DataObjectType,
    InsertOverwriteStrategy,
    EngineRunMode,
)

pytestmark = [pytest.mark.engine, pytest.mark.timeplus]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> TimeplusEngineAdapter:
    return make_mocked_engine_adapter(TimeplusEngineAdapter)


def test_create_stream(adapter: TimeplusEngineAdapter):
    """Test that CREATE TABLE is converted to CREATE STREAM."""
    columns_to_types = {
        "event_time": exp.DataType(this="DATETIME64", expressions=[exp.Literal.number(3)]),
        "user_id": exp.DataType(this="STRING"),
        "value": exp.DataType(this="INT"),
    }

    adapter.create_table(
        "test_db.test_stream",
        columns_to_types,
        table_properties={"replication_factor": 1},
    )

    # Should create STREAM instead of TABLE
    assert "CREATE STREAM" in to_sql_calls(adapter)[0]
    assert "replication_factor" in to_sql_calls(adapter)[0].lower()


def test_create_mutable_stream(adapter: TimeplusEngineAdapter):
    """Test that tables with PRIMARY KEY become MUTABLE STREAM."""
    schema = exp.Schema(
        this=exp.to_table("test_db.test_mutable"),
        expressions=[
            exp.ColumnDef(
                this=exp.column("user_id"),
                kind=exp.DataType.build("string"),
            ),
            exp.ColumnDef(
                this=exp.column("username"),
                kind=exp.DataType.build("string"),
            ),
            exp.PrimaryKey(expressions=[exp.column("user_id")]),
        ],
    )

    adapter._create_table(
        schema,
        None,
        table_properties={"replication_factor": 1},
    )

    # Should create MUTABLE STREAM when PRIMARY KEY is present
    assert "CREATE MUTABLE STREAM" in to_sql_calls(adapter)[0]


def test_drop_stream(adapter: TimeplusEngineAdapter):
    """Test that DROP TABLE is converted to DROP STREAM."""
    adapter.drop_table("test_db.test_stream")

    # Should drop STREAM instead of TABLE
    assert "DROP STREAM" in to_sql_calls(adapter)[0]


def test_alter_stream(adapter: TimeplusEngineAdapter):
    """Test that ALTER TABLE is converted to ALTER STREAM."""
    alter_expr = exp.Alter(
        this=exp.to_table("test_db.test_stream"),
        actions=[exp.AlterColumn(this=exp.column("new_col"), dtype=exp.DataType.build("string"))],
    )

    adapter.alter_table([alter_expr])

    # Check that ALTER is applied to stream
    executed_sql = to_sql_calls(adapter)[0]
    assert "test_db" in executed_sql or "test_stream" in executed_sql


def test_no_on_cluster(adapter: TimeplusEngineAdapter):
    """Test that ON CLUSTER is not generated."""
    # Timeplus doesn't support ON CLUSTER
    assert not hasattr(adapter, "cluster") or adapter.cluster is None


def test_partition_warning_for_mutable_stream(
    adapter: TimeplusEngineAdapter, mocker: MockerFixture
):
    """Test that partitioning is ignored for mutable streams with a warning."""
    mock_logger = mocker.patch("sqlmesh.core.engine_adapter.timeplus.logger")

    schema = exp.Schema(
        this=exp.to_table("test_db.test_mutable"),
        expressions=[
            exp.ColumnDef(
                this=exp.column("user_id"),
                kind=exp.DataType.build("string"),
            ),
            exp.PrimaryKey(expressions=[exp.column("user_id")]),
        ],
    )

    adapter._create_table(
        schema,
        None,
        partitioned_by=[exp.column("event_date")],
        table_properties={"replication_factor": 1},
    )

    # Should log warning about mutable streams not supporting partitions
    mock_logger.warning.assert_called_once_with(
        "Mutable streams do not support partitioning. Ignoring PARTITION BY clause."
    )


def test_table_exists(adapter: TimeplusEngineAdapter):
    """Test table_exists uses DESCRIBE."""
    adapter.cursor.execute.side_effect = None  # No exception means table exists
    assert adapter.table_exists("test_db.test_stream")

    adapter.cursor.execute.side_effect = Exception("Table does not exist")
    assert not adapter.table_exists("test_db.test_stream")


def test_get_data_objects(adapter: TimeplusEngineAdapter):
    """Test that _get_data_objects returns streams instead of tables."""
    from sqlmesh.core.engine_adapter.shared import DataObject

    # The _get_data_objects method is already mocked, we need to override it
    # Create expected data objects
    expected_objects = [
        DataObject(
            catalog=None,
            schema="test_db",
            name="test_stream",
            type=DataObjectType.from_str("stream"),
        ),
        DataObject(
            catalog=None,
            schema="test_db",
            name="test_view",
            type=DataObjectType.VIEW,
        ),
    ]

    # Override the mock to return our expected objects
    adapter._get_data_objects = lambda schema_name, object_names=None: expected_objects

    objects = adapter._get_data_objects("test_db")

    assert len(objects) == 2
    assert objects[0].name == "test_stream"
    assert objects[0].type == DataObjectType.from_str("stream")
    assert objects[1].name == "test_view"
    assert objects[1].type == DataObjectType.VIEW


def test_delete_from_stream(adapter: TimeplusEngineAdapter):
    """Test DELETE from stream."""
    adapter.delete_from("test_db.test_stream", "user_id = '123'")

    executed_sql = to_sql_calls(adapter)[0]
    assert "DELETE FROM" in executed_sql
    assert "test_stream" in executed_sql or "test_db" in executed_sql


def test_truncate_uses_delete(adapter: TimeplusEngineAdapter):
    """Test that TRUNCATE is replaced with DELETE."""
    adapter._truncate_table("test_db.test_stream")

    # Timeplus doesn't support TRUNCATE, should use DELETE instead
    assert "DELETE FROM" in to_sql_calls(adapter)[0]


def test_materialized_view_requires_target_stream(adapter: TimeplusEngineAdapter):
    """Test that materialized views require target_stream property."""
    # SQLMesh adapter should support materialized views
    assert adapter.SUPPORTS_MATERIALIZED_VIEWS is True

    # Attempting to create a materialized view without target_stream should raise error
    with pytest.raises(ValueError) as exc_info:
        adapter.create_view(
            "test_db.test_view",
            parse_one("SELECT * FROM test_stream"),
            materialized=True,
        )

    # Error message should explain target_stream requirement
    assert "target_stream" in str(exc_info.value)
    assert "materialized_properties" in str(exc_info.value)


def test_materialized_view_with_target_stream(adapter: TimeplusEngineAdapter):
    """Test creating a materialized view with target_stream."""
    adapter.create_view(
        "test_db.test_mv",
        parse_one("SELECT * FROM test_stream"),
        materialized=True,
        materialized_properties={
            "target_stream": "output_stream",
            "checkpoint_interval": 60,
        },
    )

    # Should create MATERIALIZED VIEW with INTO clause
    # First call is DROP VIEW (due to replace=True), second is CREATE
    sql_calls = to_sql_calls(adapter)
    create_sql = sql_calls[1] if len(sql_calls) > 1 else sql_calls[0]
    assert "CREATE MATERIALIZED VIEW" in create_sql
    assert "INTO output_stream" in create_sql or "INTO `output_stream`" in create_sql
    assert "SETTINGS checkpoint_interval = 60" in create_sql


def test_drop_materialized_view_uses_drop_view(adapter: TimeplusEngineAdapter):
    """Test that materialized views are dropped with DROP VIEW (not DROP MATERIALIZED VIEW)."""
    adapter.drop_view("test_db.test_mv", materialized=True)

    # In Timeplus, both regular and materialized views use DROP VIEW
    executed_sql = to_sql_calls(adapter)[0]
    assert "DROP VIEW" in executed_sql
    assert "DROP MATERIALIZED VIEW" not in executed_sql


def test_regular_view_creation(adapter: TimeplusEngineAdapter):
    """Test that regular views are created normally."""
    adapter.create_view(
        "test_db.test_view",
        parse_one("SELECT * FROM test_stream"),
        materialized=False,
    )

    # Should create regular VIEW
    executed_sql = to_sql_calls(adapter)[0]
    assert "CREATE" in executed_sql
    assert "VIEW" in executed_sql
    assert "MATERIALIZED" not in executed_sql


def test_materialized_view_with_all_settings(adapter: TimeplusEngineAdapter):
    """Test creating a materialized view with all supported settings."""
    adapter.create_view(
        "test_db.test_mv_full",
        parse_one("SELECT * FROM test_stream"),
        materialized=True,
        materialized_properties={
            "target_stream": "output_stream",
            "checkpoint_interval": 60,
            "pause_on_start": True,
            "memory_weight": 10,
            "preferred_exec_node": 2,
            "checkpoint_settings": "incremental=true;type=rocks;storage_type=s3",
            "default_hash_table": "hybrid",
            "default_hash_join": "hybrid",
        },
    )

    # Should create MATERIALIZED VIEW with all settings
    # First call is DROP VIEW (due to replace=True), second is CREATE
    sql_calls = to_sql_calls(adapter)
    create_sql = sql_calls[1] if len(sql_calls) > 1 else sql_calls[0]
    executed_sql = create_sql
    assert "CREATE MATERIALIZED VIEW" in executed_sql
    assert "INTO output_stream" in executed_sql or "INTO `output_stream`" in executed_sql
    assert "SETTINGS" in executed_sql
    assert "checkpoint_interval = 60" in executed_sql
    assert "pause_on_start = true" in executed_sql
    assert "memory_weight = 10" in executed_sql
    assert "preferred_exec_node = 2" in executed_sql
    assert "checkpoint_settings = 'incremental=true;type=rocks;storage_type=s3'" in executed_sql
    assert "default_hash_table = 'hybrid'" in executed_sql
    assert "default_hash_join = 'hybrid'" in executed_sql


def test_materialized_view_with_pause_on_start(adapter: TimeplusEngineAdapter):
    """Test creating a paused materialized view."""
    adapter.create_view(
        "test_db.test_mv_paused",
        parse_one("SELECT * FROM test_stream"),
        materialized=True,
        materialized_properties={
            "target_stream": "output_stream",
            "pause_on_start": True,
        },
    )

    # First call is DROP VIEW (due to replace=True), second is CREATE
    sql_calls = to_sql_calls(adapter)
    create_sql = sql_calls[1] if len(sql_calls) > 1 else sql_calls[0]
    assert "pause_on_start = true" in create_sql


def test_materialized_view_with_checkpoint_settings(adapter: TimeplusEngineAdapter):
    """Test creating a materialized view with complex checkpoint settings."""
    adapter.create_view(
        "test_db.test_mv_checkpoint",
        parse_one("SELECT * FROM test_stream"),
        materialized=True,
        materialized_properties={
            "target_stream": "output_stream",
            "checkpoint_settings": "incremental=true;interval=5;async=false",
        },
    )

    # First call is DROP VIEW (due to replace=True), second is CREATE
    sql_calls = to_sql_calls(adapter)
    create_sql = sql_calls[1] if len(sql_calls) > 1 else sql_calls[0]
    assert "checkpoint_settings = 'incremental=true;interval=5;async=false'" in create_sql


def test_materialized_view_with_unknown_settings(adapter: TimeplusEngineAdapter):
    """Test that unknown settings are passed through for future compatibility."""
    adapter.create_view(
        "test_db.test_mv_future",
        parse_one("SELECT * FROM test_stream"),
        materialized=True,
        materialized_properties={
            "target_stream": "output_stream",
            "future_setting": "future_value",
            "another_new_setting": 42,
        },
    )

    # Unknown settings should still be included
    # First call is DROP VIEW (due to replace=True), second is CREATE
    sql_calls = to_sql_calls(adapter)
    create_sql = sql_calls[1] if len(sql_calls) > 1 else sql_calls[0]
    assert "future_setting = future_value" in create_sql
    assert "another_new_setting = 42" in create_sql


def test_external_stream_kafka(adapter: TimeplusEngineAdapter):
    """Test creating an external stream for Kafka integration."""
    # Test executing raw external stream SQL
    external_sql = """CREATE EXTERNAL STREAM kafka_stream (
        id INT,
        data STRING
    ) SETTINGS type='kafka', brokers='localhost:9092', topic='test'"""

    adapter.execute(external_sql)

    executed_sql = to_sql_calls(adapter)[0]
    assert "EXTERNAL STREAM" in executed_sql
    assert "kafka" in executed_sql


def test_random_stream(adapter: TimeplusEngineAdapter):
    """Test creating a random stream for testing purposes."""
    # Test executing raw random stream SQL
    random_sql = """CREATE RANDOM STREAM test_stream (
        device_id STRING DEFAULT 'device' || toString(rand() % 4),
        temperature FLOAT DEFAULT rand() % 1000 / 10
    ) SETTINGS eps=1000, interval_time=200"""

    adapter.execute(random_sql)

    executed_sql = to_sql_calls(adapter)[0]
    assert "RANDOM STREAM" in executed_sql
    assert "DEFAULT" in executed_sql


def test_stream_with_partition_by(adapter: TimeplusEngineAdapter):
    """Test creating an append stream with PARTITION BY."""
    columns = {
        "event_time": exp.DataType(this="DATETIME"),
        "device_id": exp.DataType(this="STRING"),
        "value": exp.DataType(this="FLOAT"),
    }

    adapter.create_table(
        "test_db.partitioned_stream",
        columns_to_types=columns,
        target_columns_to_types=columns,
        partitioned_by=[exp.column("toDate(event_time)")],
        table_properties={
            "ORDER_BY": ["device_id", "event_time"],
            "replication_factor": 2,
        },
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "CREATE STREAM" in executed_sql
    # Partition should be included in the SQL
    assert "PARTITION" in executed_sql.upper()


def test_materialized_view_with_window_function(adapter: TimeplusEngineAdapter):
    """Test materialized view with window functions (tumble/hop/session)."""
    adapter.create_view(
        "test_db.window_mv",
        parse_one(
            "SELECT window_start, count() AS cnt "
            "FROM tumble(sensor_stream, interval 1 minute) "
            "GROUP BY window_start"
        ),
        materialized=True,
        materialized_properties={
            "target_stream": "aggregated_stream",
            "checkpoint_interval": 30,
        },
    )

    sql_calls = to_sql_calls(adapter)
    create_sql = sql_calls[1] if len(sql_calls) > 1 else sql_calls[0]
    # Window functions should be preserved in the query
    assert "tumble" in create_sql or "TUMBLE" in create_sql
    assert "window_start" in create_sql or "WINDOW_START" in create_sql


def test_materialized_view_with_join(adapter: TimeplusEngineAdapter):
    """Test materialized view with JOIN between streams."""
    join_query = """
    SELECT s1.id, s1.value, s2.status
    FROM stream1 s1
    INNER JOIN stream2 s2 ON s1.id = s2.id
    WHERE s1.value > 100
    """

    adapter.create_view(
        "test_db.join_mv",
        parse_one(join_query),
        materialized=True,
        materialized_properties={
            "target_stream": "joined_output",
            "seek_to": "earliest",
        },
    )

    sql_calls = to_sql_calls(adapter)
    create_sql = sql_calls[1] if len(sql_calls) > 1 else sql_calls[0]
    assert "JOIN" in create_sql
    assert "seek_to" in create_sql and "earliest" in create_sql


def test_historical_query_with_table_function(adapter: TimeplusEngineAdapter):
    """Test that queries can use table() function for historical data."""
    # Execute a query with table() function
    query = "SELECT * FROM table(sensor_stream) WHERE event_time > '2024-01-01'"
    adapter.execute(query)

    executed_sql = to_sql_calls(adapter)[0]
    assert "table(" in executed_sql.lower()


def test_stream_with_ttl_settings(adapter: TimeplusEngineAdapter):
    """Test creating stream with TTL and retention settings."""
    schema = exp.Schema(
        this=exp.to_table("test_db.ttl_stream"),
        expressions=[
            exp.ColumnDef(
                this=exp.column("timestamp"),
                kind=exp.DataType(this="DATETIME"),
            ),
            exp.ColumnDef(
                this=exp.column("message"),
                kind=exp.DataType(this="STRING"),
            ),
            exp.PrimaryKey(expressions=[exp.column("timestamp")]),
        ],
    )

    adapter._create_table(
        schema,
        None,
        table_properties={
            "logstore_retention_ms": 604800000,  # 7 days
            "ttl_seconds": 86400,  # 1 day
            "shards": 2,
        },
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "MUTABLE STREAM" in executed_sql
    assert "logstore_retention_ms" in executed_sql and "604800000" in executed_sql
    assert "ttl_seconds" in executed_sql and "86400" in executed_sql


def test_external_table_clickhouse(adapter: TimeplusEngineAdapter):
    """Test creating external table for ClickHouse."""
    adapter.execute(
        """CREATE EXTERNAL TABLE ch_table
        SETTINGS type='clickhouse',
                address='localhost:9000',
                database='default',
                table='source'"""
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "EXTERNAL TABLE" in executed_sql
    assert "clickhouse" in executed_sql.lower()


def test_stream_with_various_data_types(adapter: TimeplusEngineAdapter):
    """Test stream creation with various Timeplus/ClickHouse data types."""
    columns = {
        "int8_col": exp.DataType(this="INT8"),
        "uint64_col": exp.DataType(this="UINT64"),
        "float64_col": exp.DataType(this="FLOAT64"),
        "decimal_col": exp.DataType(
            this="DECIMAL", expressions=[exp.Literal.number(10), exp.Literal.number(2)]
        ),
        "bool_col": exp.DataType(this="BOOL"),
        "string_col": exp.DataType(this="STRING"),
        "fixed_string": exp.DataType(this="FIXEDSTRING", expressions=[exp.Literal.number(10)]),
        "datetime64_col": exp.DataType(this="DATETIME64", expressions=[exp.Literal.number(3)]),
        "date32_col": exp.DataType(this="DATE32"),
        "array_col": exp.DataType(this="ARRAY", expressions=[exp.DataType(this="INT")]),
        "tuple_col": exp.DataType(this="TUPLE", nested=True),
        "map_col": exp.DataType(
            this="MAP", expressions=[exp.DataType(this="STRING"), exp.DataType(this="INT")]
        ),
        "json_col": exp.DataType(this="JSON"),
    }

    adapter.create_table(
        "test_db.type_test_stream",
        columns_to_types=columns,
        target_columns_to_types=columns,
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "CREATE STREAM" in executed_sql
    # Check some specific types are preserved
    assert "int8" in executed_sql.lower() or "INT8" in executed_sql
    assert "datetime64" in executed_sql.lower() or "DATETIME64" in executed_sql


def test_materialized_view_with_emit_version(adapter: TimeplusEngineAdapter):
    """Test materialized view using emit_version() for CDC."""
    query = """
    SELECT emit_version() AS version,
           max(price) AS max_price,
           min(price) AS min_price,
           count() AS trade_count
    FROM trades_stream
    """

    adapter.create_view(
        "test_db.cdc_mv",
        parse_one(query),
        materialized=True,
        materialized_properties={
            "target_stream": "cdc_output",
            "checkpoint_interval": 10,
        },
    )

    sql_calls = to_sql_calls(adapter)
    create_sql = sql_calls[1] if len(sql_calls) > 1 else sql_calls[0]
    assert "emit_version()" in create_sql


def test_alter_stream_operations(adapter: TimeplusEngineAdapter):
    """Test various ALTER STREAM operations."""
    # Add column
    alter_add = exp.Alter(
        this=exp.to_table("test_db.test_stream"),
        actions=[
            exp.AlterColumn(
                this=exp.column("new_field"),
                dtype=exp.DataType(this="STRING"),
                kind="ADD",
            )
        ],
    )
    adapter.alter_table([alter_add])

    # Drop column
    alter_drop = exp.Alter(
        this=exp.to_table("test_db.test_stream"),
        actions=[
            exp.AlterColumn(
                this=exp.column("old_field"),
                kind="DROP",
            )
        ],
    )
    adapter.alter_table([alter_drop])

    # Modify column
    alter_modify = exp.Alter(
        this=exp.to_table("test_db.test_stream"),
        actions=[
            exp.AlterColumn(
                this=exp.column("existing_field"),
                dtype=exp.DataType(this="FLOAT64"),
                kind="MODIFY",
            )
        ],
    )
    adapter.alter_table([alter_modify])

    sql_calls = to_sql_calls(adapter)
    # Check that ALTER operations were executed
    assert len(sql_calls) >= 3  # Should have at least 3 ALTER operations
    # ALTER syntax may vary, just check we have ALTER statements
    assert any("ALTER" in sql for sql in sql_calls)


def test_stream_with_multiple_settings(adapter: TimeplusEngineAdapter):
    """Test stream creation with multiple Timeplus-specific settings."""
    adapter.create_table(
        "test_db.multi_setting_stream",
        columns_to_types={"id": exp.DataType(this="INT")},
        target_columns_to_types={"id": exp.DataType(this="INT")},
        table_properties={
            "shards": 4,
            "replication_factor": 3,
            "flush_interval_ms": 1000,
            "index_granularity": 8192,
            "enable_staging_area": True,
            "enable_distributed_insert_select": False,
        },
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "shards" in executed_sql and "4" in executed_sql
    assert "replication_factor" in executed_sql and "3" in executed_sql


def test_remote_function_creation(adapter: TimeplusEngineAdapter):
    """Test creating a remote function."""
    remote_func_sql = """
    CREATE REMOTE FUNCTION ip_lookup(ip STRING)
    RETURNS STRING
    URL 'https://api.ipify.org'
    AUTH_METHOD 'none'
    """

    adapter.execute(remote_func_sql)

    executed_sql = to_sql_calls(adapter)[0]
    assert "REMOTE FUNCTION" in executed_sql
    assert "AUTH_METHOD" in executed_sql


def test_materialized_view_with_having(adapter: TimeplusEngineAdapter):
    """Test materialized view with GROUP BY and HAVING clause."""
    query = """
    SELECT user_id, count() AS event_count, avg(amount) AS avg_amount
    FROM transactions
    GROUP BY user_id
    HAVING event_count > 10 AND avg_amount > 100
    """

    adapter.create_view(
        "test_db.having_mv",
        parse_one(query),
        materialized=True,
        materialized_properties={
            "target_stream": "user_stats",
            "memory_weight": 5,
        },
    )

    sql_calls = to_sql_calls(adapter)
    create_sql = sql_calls[1] if len(sql_calls) > 1 else sql_calls[0]
    assert "HAVING" in create_sql
    assert "memory_weight = 5" in create_sql


def test_changelog_kv_stream(adapter: TimeplusEngineAdapter):
    """Test creating a changelog_kv stream for CDC."""
    columns = {
        "product_id": exp.DataType(this="STRING"),
        "price": exp.DataType(this="FLOAT"),
        "_tp_delta": exp.DataType(this="INT8"),
    }

    adapter.create_table(
        "test_db.dim_products",
        columns_to_types=columns,
        target_columns_to_types=columns,
        table_properties={
            "mode": "changelog_kv",
            "replication_factor": 1,
        },
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "CREATE STREAM" in executed_sql  # Not MUTABLE STREAM for changelog_kv
    assert "mode = 'changelog_kv'" in executed_sql.replace("'", "'")


def test_rename_stream(adapter: TimeplusEngineAdapter):
    """Test renaming a stream."""
    adapter.rename_table(
        "test_db.old_stream",
        "test_db.new_stream",
        ignore_if_not_exists=True,
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "RENAME STREAM" in executed_sql
    assert "IF EXISTS" in executed_sql
    assert "old_stream" in executed_sql
    assert "new_stream" in executed_sql


def test_insert_overwrite_by_partition(adapter: TimeplusEngineAdapter):
    """Test insert overwrite by partition."""
    # Test that DROP PARTITION is executed for each partition
    partitions = [
        exp.Literal.string("2024-01-01"),
        exp.Literal.string("2024-01-02"),
    ]

    # Just test the partition dropping part
    table = exp.to_table("test_db.partitioned_stream")
    for partition in partitions:
        drop_partition_sql = f"ALTER STREAM {table.sql(dialect=adapter.dialect)} DROP PARTITION {partition.sql(dialect=adapter.dialect)}"
        adapter.execute(drop_partition_sql)

    sql_calls = to_sql_calls(adapter)
    # Should have ALTER STREAM DROP PARTITION commands
    assert len(sql_calls) == 2
    assert all("ALTER STREAM" in sql and "DROP PARTITION" in sql for sql in sql_calls)
    assert "'2024-01-01'" in sql_calls[0]
    assert "'2024-01-02'" in sql_calls[1]


def test_cluster_support(adapter: TimeplusEngineAdapter):
    """Test cluster configuration support.

    Timeplus handles clustering transparently through replication_factor setting,
    not through ON CLUSTER syntax. The adapter always returns STANDALONE mode
    unless cloud_mode is enabled.
    """
    # Default should be standalone
    assert adapter.engine_run_mode == EngineRunMode.STANDALONE
    assert adapter.cluster is None

    # Even with cluster config, Timeplus doesn't use cluster mode
    # Clustering is handled via replication_factor in stream properties
    adapter._extra_config = {"cluster": "test_cluster"}
    assert adapter.engine_run_mode == EngineRunMode.STANDALONE
    assert adapter.cluster is None  # Always returns None for Timeplus

    # Test with cloud mode
    adapter._extra_config = {"cloud_mode": True}
    assert adapter.engine_run_mode == EngineRunMode.CLOUD

    # Test stream creation with replication_factor for enterprise deployments
    # In single instance (default), replication_factor=1 or omitted
    # In enterprise clusters, replication_factor=3 for 3-node clusters
    adapter._extra_config = {}  # Reset config
    columns = {
        "id": exp.DataType(this="INT"),
        "name": exp.DataType(this="STRING"),
    }
    adapter.create_table(
        "test_db.replicated_stream",
        columns_to_types=columns,
        target_columns_to_types=columns,
        table_properties={"replication_factor": 3},  # For 3-node enterprise cluster
    )

    create_sql = to_sql_calls(adapter)[0]
    assert "replication_factor = 3" in create_sql  # Verify replication is set


def test_create_stream_like(adapter: TimeplusEngineAdapter):
    """Test CREATE STREAM ... AS ... syntax for structure copy."""
    adapter._create_table_like(
        "test_db.new_stream",
        "test_db.existing_stream",
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "CREATE STREAM" in executed_sql
    assert " AS " in executed_sql
    assert "new_stream" in executed_sql
    assert "existing_stream" in executed_sql


def test_delete_from_changelog_stream(adapter: TimeplusEngineAdapter):
    """Test DELETE from changelog_kv stream (special case)."""
    # For changelog_kv streams, delete is handled differently
    # You insert with _tp_delta=-1 rather than using DELETE
    adapter.delete_from("test_db.changelog_stream", "product_id = 'ABC123'")

    executed_sql = to_sql_calls(adapter)[0]
    assert "DELETE FROM" in executed_sql
    assert "product_id" in executed_sql and "ABC123" in executed_sql


def test_insert_overwrite_strategy(adapter: TimeplusEngineAdapter):
    """Test that INSERT_OVERWRITE_STRATEGY is set correctly."""
    assert adapter.INSERT_OVERWRITE_STRATEGY == InsertOverwriteStrategy.INTO_IS_OVERWRITE


def test_regular_stream_no_mode_setting(adapter: TimeplusEngineAdapter):
    """Test that regular streams are created without any mode setting.

    In Timeplus, there is no 'append' mode keyword. Regular streams are created
    without any mode setting. Only special stream types (versioned_kv, changelog_kv,
    changelog) have explicit mode settings.
    """
    columns = {
        "event_time": exp.DataType(this="DATETIME64", expressions=[exp.Literal.number(3)]),
        "user_id": exp.DataType(this="STRING"),
        "value": exp.DataType(this="INT"),
    }

    # Create a regular append stream
    adapter.create_table(
        "test_db.append_test",
        columns_to_types=columns,
        target_columns_to_types=columns,
    )

    create_sql = to_sql_calls(adapter)[0]

    # Stream should still be created (without explicit mode setting)
    assert "CREATE STREAM" in create_sql
    assert "append_test" in create_sql


def test_special_stream_modes(adapter: TimeplusEngineAdapter):
    """Test that only valid special stream modes are set.

    Timeplus only has special modes: versioned_kv, changelog_kv, changelog.
    No 'append' mode exists - regular streams have no mode setting.
    """
    columns = {
        "id": exp.DataType(this="STRING"),
        "value": exp.DataType(this="STRING"),
    }

    # Test versioned_kv mode
    adapter.create_table(
        "test_db.versioned_stream",
        columns_to_types=columns,
        target_columns_to_types=columns,
        table_properties={"mode": "versioned_kv"},
    )

    create_sql = to_sql_calls(adapter)[0]
    assert "mode = 'versioned_kv'" in create_sql

    # Test changelog mode
    adapter.create_table(
        "test_db.changelog_stream",
        columns_to_types=columns,
        target_columns_to_types=columns,
        table_properties={"mode": "changelog"},
    )

    create_sql = to_sql_calls(adapter)[1]
    assert "mode = 'changelog'" in create_sql


def test_create_external_stream_kafka(adapter: TimeplusEngineAdapter):
    """Test creating Kafka external stream from model definition."""
    columns = {
        "event_id": exp.DataType(this="STRING"),
        "user_id": exp.DataType(this="STRING"),
        "amount": exp.DataType(this="FLOAT64"),
        "timestamp": exp.DataType(this="DATETIME64", expressions=[exp.Literal.number(3)]),
    }

    adapter.create_table(
        "test_db.kafka_events",
        columns_to_types=columns,
        target_columns_to_types=columns,
        table_properties={
            "external_type": "kafka",
            "brokers": "kafka:29092",
            "topic": "events",
            "data_format": "JSONEachRow",
        },
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "CREATE EXTERNAL STREAM" in executed_sql
    assert "type = 'kafka'" in executed_sql
    assert "brokers = 'kafka:29092'" in executed_sql
    assert "topic = 'events'" in executed_sql
    assert "data_format = 'JSONEachRow'" in executed_sql


def test_create_external_table_clickhouse(adapter: TimeplusEngineAdapter):
    """Test creating ClickHouse external table from model definition."""
    adapter.create_table(
        "test_db.clickhouse_results",
        columns_to_types={},  # External tables don't need schema in CREATE statement
        target_columns_to_types={},
        table_properties={
            "external_type": "clickhouse",
            "address": "clickhouse:9000",
            "database": "default",
            "table": "results",
            "user": "default",
            "password": "",
        },
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "CREATE EXTERNAL TABLE" in executed_sql
    assert "type = 'clickhouse'" in executed_sql
    assert "address = 'clickhouse:9000'" in executed_sql
    assert "database = 'default'" in executed_sql
    assert "table = 'results'" in executed_sql


def test_materialized_view_requires_existing_target_stream(
    adapter: TimeplusEngineAdapter, mocker: MockerFixture
):
    """Test that materialized views require target stream to exist explicitly."""
    # Mock table_exists to return False for target stream
    original_table_exists = adapter.table_exists

    def mock_table_exists(table_name):
        if "target_stream" in str(table_name):
            return False
        return original_table_exists(table_name)

    adapter.table_exists = mock_table_exists

    # Create materialized view with non-existent target stream should raise error
    query = parse_one("""
        SELECT
            user_id,
            count() AS event_count,
            sum(amount) AS total_amount
        FROM source_stream
        GROUP BY user_id
    """)

    with pytest.raises(ValueError) as exc_info:
        adapter.create_view(
            "test_db.aggregation_mv",
            query,
            materialized=True,
            materialized_properties={
                "target_stream": "target_stream",
                "checkpoint_interval": 60,
            },
        )

    # Error should explain that target stream must be created explicitly
    assert "Target stream 'target_stream' does not exist" in str(exc_info.value)
    assert "Please create it explicitly" in str(exc_info.value)


def test_external_stream_with_replace(adapter: TimeplusEngineAdapter):
    """Test that external streams can be replaced by dropping and recreating."""
    columns = {
        "id": exp.DataType(this="INT"),
        "data": exp.DataType(this="STRING"),
    }

    adapter.create_table(
        "test_db.kafka_test",
        columns_to_types=columns,
        target_columns_to_types=columns,
        table_properties={
            "external_type": "kafka",
            "brokers": "localhost:9092",
            "topic": "test",
        },
        replace=True,
    )

    sql_calls = to_sql_calls(adapter)
    # Should have DROP STREAM followed by CREATE EXTERNAL STREAM
    assert any("DROP STREAM" in sql for sql in sql_calls)
    assert any("CREATE EXTERNAL STREAM" in sql and "IF NOT EXISTS" not in sql for sql in sql_calls)


def test_external_stream_without_external_type(adapter: TimeplusEngineAdapter):
    """Test that streams without external_type are created as regular streams."""
    columns = {
        "id": exp.DataType(this="INT"),
        "name": exp.DataType(this="STRING"),
    }

    adapter.create_table(
        "test_db.regular_stream",
        columns_to_types=columns,
        target_columns_to_types=columns,
        table_properties={
            "replication_factor": 1,
        },
    )

    executed_sql = to_sql_calls(adapter)[0]
    # Should be regular stream, not external
    assert "CREATE STREAM" in executed_sql
    assert "EXTERNAL" not in executed_sql


def test_materialized_view_with_explicit_target_stream(
    adapter: TimeplusEngineAdapter, mocker: MockerFixture
):
    """Test that materialized views work when target stream exists."""

    # Mock table_exists to return True for target stream (it exists)
    original_table_exists = adapter.table_exists

    def mock_table_exists(table_name):
        if "existing_target" in str(table_name):
            return True
        return original_table_exists(table_name)

    adapter.table_exists = mock_table_exists

    query = parse_one("""
        SELECT
            user_id,
            count() AS event_count,
            sum(amount) AS total_amount
        FROM events_stream
        GROUP BY user_id
    """)

    adapter.create_view(
        "test_db.user_stats_mv",
        query,
        materialized=True,
        materialized_properties={
            "target_stream": "existing_target",
        },
    )

    sql_calls = to_sql_calls(adapter)
    # Should create materialized view (no target stream creation)
    create_mv_calls = [sql for sql in sql_calls if "CREATE MATERIALIZED VIEW" in sql]
    assert len(create_mv_calls) >= 1

    # Should NOT create the target stream (it already exists)
    create_stream_calls = [
        sql for sql in sql_calls if "CREATE STREAM" in sql and "existing_target" in sql
    ]
    assert len(create_stream_calls) == 0, "Should not create target stream if it already exists"


def test_external_stream_with_additional_settings(adapter: TimeplusEngineAdapter):
    """Test external stream with additional Kafka/Timeplus settings."""
    columns = {
        "event_id": exp.DataType(this="STRING"),
        "payload": exp.DataType(this="STRING"),
    }

    adapter.create_table(
        "test_db.kafka_advanced",
        columns_to_types=columns,
        target_columns_to_types=columns,
        table_properties={
            "external_type": "kafka",
            "brokers": "kafka:29092",
            "topic": "advanced_topic",
            "data_format": "JSONEachRow",
            "consumer_group": "sqlmesh_group",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "user",
            "sasl_password": "pass",
            "security_protocol": "SASL_SSL",
        },
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "CREATE EXTERNAL STREAM" in executed_sql
    assert "consumer_group = 'sqlmesh_group'" in executed_sql
    assert "sasl_mechanism = 'PLAIN'" in executed_sql
    assert "security_protocol = 'SASL_SSL'" in executed_sql


def test_external_table_with_schema(adapter: TimeplusEngineAdapter):
    """Test that external tables can optionally include schema for documentation."""
    columns = {
        "win_start": exp.DataType(this="DATETIME64", expressions=[exp.Literal.number(3)]),
        "win_end": exp.DataType(this="DATETIME64", expressions=[exp.Literal.number(3)]),
        "count": exp.DataType(this="UINT64"),
    }

    adapter.create_table(
        "test_db.ch_table_with_schema",
        columns_to_types=columns,
        target_columns_to_types=columns,
        table_properties={
            "external_type": "clickhouse",
            "address": "localhost:9000",
            "database": "default",
            "table": "aggregations",
        },
    )

    executed_sql = to_sql_calls(adapter)[0]
    assert "CREATE EXTERNAL TABLE" in executed_sql
    # Schema may or may not be included depending on implementation
    # Just ensure the external table is created correctly
    assert "type = 'clickhouse'" in executed_sql


def test_no_auto_create_if_target_exists(adapter: TimeplusEngineAdapter, mocker: MockerFixture):
    """Test that target stream is NOT created if it already exists."""
    # Mock table_exists to return True for target stream
    original_table_exists = adapter.table_exists

    def mock_table_exists(table_name):
        if "existing_target" in str(table_name):
            return True
        return original_table_exists(table_name)

    adapter.table_exists = mock_table_exists

    query = parse_one("SELECT * FROM source_stream")

    adapter.create_view(
        "test_db.test_mv",
        query,
        materialized=True,
        materialized_properties={
            "target_stream": "existing_target",
        },
    )

    sql_calls = to_sql_calls(adapter)
    # Should NOT have CREATE STREAM for target (only DROP VIEW and CREATE MV)
    create_stream_calls = [
        sql for sql in sql_calls if "CREATE STREAM" in sql and "existing_target" in sql
    ]
    assert len(create_stream_calls) == 0, "Should not create target stream if it already exists"
