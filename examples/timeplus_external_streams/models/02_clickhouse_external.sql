-- ClickHouse External Table
-- References the ClickHouse table for writing aggregated data

MODEL (
    name default.clickhouse_results,
    kind FULL,
    dialect timeplus,
    depends_on [default.e2e_aggregation_results],
    physical_properties (
        external_type = 'clickhouse',
        address = 'clickhouse:9000',
        database = 'default',
        table = 'e2e_aggregation_results'
    ),
    columns (
        win_start datetime64(3),
        win_end datetime64(3),
        user_id string,
        event_type string,
        region string,
        event_count uint64,
        total_amount float64,
        avg_amount float64,
        min_amount float64,
        max_amount float64,
        inserted_at datetime64(3)
    ),
    description 'ClickHouse external table for writing aggregations'
);

-- Empty query placeholder
SELECT
    now64(3) AS win_start,
    now64(3) AS win_end,
    '' AS user_id,
    '' AS event_type,
    '' AS region,
    CAST(0 AS uint64) AS event_count,
    0.0 AS total_amount,
    0.0 AS avg_amount,
    0.0 AS min_amount,
    0.0 AS max_amount,
    now64(3) AS inserted_at
WHERE false;
