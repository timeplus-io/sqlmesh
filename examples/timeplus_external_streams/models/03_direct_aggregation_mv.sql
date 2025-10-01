-- Direct Aggregation Materialized View
-- Aggregates Kafka events and writes directly to ClickHouse
-- No intermediate streams needed!

MODEL (
    name default.direct_to_clickhouse_mv,
    kind VIEW (
        materialized true
    ),
    dialect timeplus,
    depends_on [default.clickhouse_results],
    physical_properties (
        target_stream = 'default.clickhouse_results',
        checkpoint_interval = 5
    ),
    description 'Direct aggregation: Kafka → ClickHouse (5-second windows)'
);

SELECT
    window_start AS win_start,
    window_end AS win_end,
    user_id,
    event_type,
    'N/A' AS region,
    count() AS event_count,
    sum(amount) AS total_amount,
    avg(amount) AS avg_amount,
    min(amount) AS min_amount,
    max(amount) AS max_amount,
    now64(3) AS inserted_at
FROM tumble(default.kafka_events_stream, timestamp, INTERVAL 5 SECOND)
GROUP BY window_start, window_end, user_id, event_type;
