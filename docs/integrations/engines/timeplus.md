# Timeplus

## Overview

[Timeplus](https://www.timeplus.com/) is a streaming-first analytics platform that combines the power of streaming and historical data processing. It is a fork of ClickHouse optimized for real-time analytics with native support for streaming workloads.

Key differences from ClickHouse:
- **Streams instead of tables** - Primary data storage uses streams (append or mutable)
- **Streaming semantics** - Built-in support for streaming windows and materialized views
- **Simplified clustering** - Uses `replication_factor` instead of `ON CLUSTER` syntax
- **Native streaming functions** - Functions use snake_case naming (e.g., `to_int32` instead of `toInt32`)

## Local/Built-in Scheduler
**Engine Adapter Type**: `timeplus`

!!! warning
    Timeplus is not recommended for use as the SQLMesh [state connection](../../reference/configuration.md#connections) due to nullable type handling issues. Use DuckDB or PostgreSQL for state storage instead.

### Installation

Install the Timeplus connector (forked from clickhouse-connect):
```bash
pip install "sqlmesh[timeplus]"
```

Or if you have a local fork of timeplus-connect:
```bash
pip install -e /path/to/timeplus-connect
pip install sqlmesh
```

### Connection Options

| Option                     | Description                                           | Type          | Required |
|----------------------------|-------------------------------------------------------|:-------------:|:--------:|
| `type`                     | Engine type name - must be `timeplus`                | string        | Y        |
| `host`                     | Hostname of the Timeplus server                      | string        | Y        |
| `username`                 | Username for authentication                          | string        | Y        |
| `password`                 | Password for authentication                          | string        | N        |
| `port`                     | Port number (default: 8463 for HTTP)                 | int           | N        |
| `database`                 | Target database                                      | string        | N        |
| `connect_timeout`          | Connection timeout in seconds (default: 10)          | int           | N        |
| `send_receive_timeout`     | Send/receive timeout in seconds (default: 300)       | int           | N        |
| `query_limit`              | Maximum number of rows to return (0 = unlimited)     | int           | N        |
| `use_compression`          | Enable compression (default: true)                   | bool          | N        |
| `compression_method`       | Compression method (lz4, zstd)                       | string        | N        |
| `connection_settings`      | Additional connection settings                       | dict          | N        |
| `http_proxy`               | HTTP proxy URL                                        | string        | N        |
| `https_proxy`              | HTTPS proxy URL                                       | string        | N        |
| `verify`                   | Verify SSL certificates (default: true)              | bool          | N        |
| `ca_cert`                  | Path to CA certificate                               | string        | N        |
| `client_cert`              | Path to client certificate                           | string        | N        |
| `client_cert_key`          | Path to client certificate key                       | string        | N        |
| `server_host_name`         | Server hostname for TLS verification                 | string        | N        |

### Configuration Example

```yaml
gateways:
  my_timeplus:
    connection:
      type: timeplus
      host: localhost
      port: 8463
      username: default
      password: ""
      database: default
      concurrent_tasks: 4
      connection_settings:
        max_execution_time: 3600
    # Use DuckDB for state storage
    state_connection:
      type: duckdb
      database: sqlmesh_state.db

default_gateway: my_timeplus

model_defaults:
  dialect: timeplus
```

## Working with Streams

### Append Streams

Append streams are the default stream type in Timeplus, optimized for high-throughput ingestion:

```sql
MODEL (
    name my_db.events_stream,
    kind FULL,
    physical_properties (
        order_by = (event_time, user_id),
        replication_factor = 1
    ),
    partitioned_by (to_yyyymm(event_time))
);

SELECT
    now64() as event_time,
    user_id,
    event_type,
    value
FROM source_events;
```

### Mutable Streams

Mutable streams support updates and deletes, using a row-based storage format:

```sql
MODEL (
    name my_db.user_profiles,
    kind FULL,
    columns (
        user_id string PRIMARY KEY,
        username string,
        email string,
        last_login datetime64(3)
    ),
    physical_properties (
        replication_factor = 1
    )
);

SELECT * FROM external_users;
```

!!! info
    Mutable streams do not support partitioning as they use RocksDB for storage instead of MergeTree.

## Data Types

Timeplus uses lowercase data type names. When writing models for Timeplus, use:
- `int8`, `int16`, `int32`, `int64` instead of `Int8`, `Int16`, etc.
- `float32`, `float64` instead of `Float32`, `Float64`
- `string` instead of `String`
- `fixed_string(N)` instead of `FixedString(N)`
- `datetime64(P)` instead of `DateTime64(P)`

## Functions

Timeplus functions use snake_case naming convention:
- `to_int32()` instead of `toInt32()`
- `to_yyyymm()` instead of `toYYYYMM()`
- `parse_datetime64_best_effort()` instead of `parseDateTime64BestEffort()`
- `now64()` for current timestamp with millisecond precision

## Replication

Timeplus doesn't use ClickHouse's `ON CLUSTER` syntax. Instead, use the `replication_factor` setting:

```sql
MODEL (
    name my_db.replicated_stream,
    physical_properties (
        replication_factor = 3  -- Create 3 replicas
    )
);
```

## Views and Materialized Views

In Timeplus, **VIEW** and **MATERIALIZED VIEW** are fundamentally different resources:

### Regular Views
- Saved queries that run when accessed
- Supported normally by SQLMesh for model management
- Created with: `CREATE VIEW view_name AS SELECT ...`

### Materialized Views
- **Continuous streaming processes** that run in the background
- Output results to a target stream via `INTO` clause
- **Supported in SQLMesh** with required `target_stream` property

#### Creating Materialized Views in SQLMesh

Timeplus materialized views are supported by specifying a `target_stream` in the model's physical properties:

```sql
-- models/realtime_aggregation.sql
MODEL (
    name analytics.user_activity_mv,
    kind VIEW,
    materialized true,
    physical_properties (
        target_stream 'user_activity_output',
        checkpoint_interval 60
    )
);

SELECT
    window_start,
    user_id,
    COUNT(*) as event_count,
    AVG(response_time) as avg_response
FROM tumble(events_stream, 1m)
GROUP BY window_start, user_id;
```

This will generate:
```sql
CREATE MATERIALIZED VIEW analytics.user_activity_mv
INTO user_activity_output
AS
SELECT
    window_start,
    user_id,
    COUNT(*) as event_count,
    AVG(response_time) as avg_response
FROM tumble(events_stream, 1m)
GROUP BY window_start, user_id
SETTINGS checkpoint_interval = 60;
```

#### Advanced Example with All Settings

```sql
-- models/advanced_mv.sql
MODEL (
    name analytics.advanced_aggregation_mv,
    kind VIEW,
    materialized true,
    physical_properties (
        target_stream 'advanced_output',
        checkpoint_interval 30,
        checkpoint_settings 'incremental=true;type=rocks;storage_type=s3',
        pause_on_start true,  -- Deploy without starting
        memory_weight 10,     -- For cluster load balancing
        preferred_exec_node 2,  -- Run on specific node
        default_hash_table 'hybrid',
        default_hash_join 'hybrid'
    )
);

SELECT
    window_start,
    region,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(revenue) as total_revenue
FROM tumble(transactions, 5m)
GROUP BY window_start, region;
```

This generates:
```sql
CREATE MATERIALIZED VIEW analytics.advanced_aggregation_mv
INTO advanced_output
AS
SELECT
    window_start,
    region,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(revenue) as total_revenue
FROM tumble(transactions, 5m)
GROUP BY window_start, region
SETTINGS checkpoint_interval = 30,
         checkpoint_settings = 'incremental=true;type=rocks;storage_type=s3',
         pause_on_start = true,
         memory_weight = 10,
         preferred_exec_node = 2,
         default_hash_table = 'hybrid',
         default_hash_join = 'hybrid';
```

#### Physical Properties for Materialized Views

| Property | Description | Required |
|----------|-------------|----------|
| `target_stream` | Name of the stream to output results to | **Yes** |
| `checkpoint_interval` | Checkpoint interval in seconds | No |
| `checkpoint_settings` | Complex checkpoint configuration (e.g., 'incremental=true;type=rocks') | No |
| `pause_on_start` | Start MV in paused state (true/false) | No |
| `memory_weight` | Weight for cluster load balancing (integer, default 0) | No |
| `preferred_exec_node` | Specific node ID to run MV on | No |
| `default_hash_table` | Hash table type ('hybrid' or 'linear') | No |
| `default_hash_join` | Hash join type ('hybrid' or 'linear') | No |


!!! note
    The target stream must exist before creating the materialized view, or you must create it manually. SQLMesh does not automatically create target streams.

!!! info
    In Timeplus, both regular views and materialized views are dropped using `DROP VIEW` syntax, not `DROP MATERIALIZED VIEW`.

## Limitations

1. **State Sync**: Cannot be used for SQLMesh state storage due to nullable type issues
2. **Transactions**: Does not support transactions
3. **Partition Limitations**: Mutable streams cannot be partitioned
4. **TRUNCATE**: Not supported, uses DELETE instead
5. **View Schema**: Cannot infer schema from views
6. **CREATE OR REPLACE**: Timeplus doesn't support CREATE OR REPLACE syntax. When replacing objects, SQLMesh will DROP and then CREATE with IF NOT EXISTS

## Migration from ClickHouse

When migrating models from ClickHouse to Timeplus:

1. **Replace TABLE with STREAM**: All tables become streams
2. **Update data types**: Convert to lowercase (e.g., `Int32` → `int32`)
3. **Update functions**: Convert to snake_case (e.g., `toInt32` → `to_int32`)
4. **Remove ON CLUSTER**: Use `replication_factor` setting instead
5. **Check partitioning**: Ensure only append streams use partitions

## Example Model

```sql
-- models/orders_summary.sql
MODEL (
    name analytics.orders_summary,
    kind INCREMENTAL_BY_TIME_RANGE,
    cron '@daily',
    start '2024-01-01',
    physical_properties (
        order_by = (order_date, customer_id),
        replication_factor = 1
    ),
    partitioned_by (to_yyyymm(order_date)),
    time_column order_date
);

SELECT
    to_date(order_timestamp) as order_date,
    customer_id,
    count() as order_count,
    sum(order_total) as total_revenue,
    avg(order_total) as avg_order_value
FROM raw.orders
WHERE
    order_timestamp BETWEEN @start_ds AND @end_ds
GROUP BY order_date, customer_id;