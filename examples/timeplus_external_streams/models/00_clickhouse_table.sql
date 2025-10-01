-- ClickHouse Table for Aggregation Results
-- Created via SQLMesh using clickhouse_local gateway

MODEL (
    name default.e2e_aggregation_results,
    kind FULL,
    dialect clickhouse,
    gateway clickhouse_local,
    storage_format 'MergeTree',
    physical_properties (
        order_by = (user_id, win_start)
    ),
    description 'ClickHouse table for receiving Timeplus aggregations'
);

-- Table structure defined by query (WHERE false means no data inserted)
SELECT
    CAST(now() AS DateTime64(3)) AS win_start,
    CAST(now() AS DateTime64(3)) AS win_end,
    CAST('' AS String) AS user_id,
    CAST('' AS String) AS event_type,
    CAST('' AS String) AS region,
    CAST(0 AS UInt64) AS event_count,
    CAST(0.0 AS Float64) AS total_amount,
    CAST(0.0 AS Float64) AS avg_amount,
    CAST(0.0 AS Float64) AS min_amount,
    CAST(0.0 AS Float64) AS max_amount,
    CAST(now() AS DateTime64(3)) AS inserted_at
WHERE false;
