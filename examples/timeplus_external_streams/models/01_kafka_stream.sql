-- Kafka External Stream
-- Reads events from Kafka topic in real-time

MODEL (
    name default.kafka_events_stream,
    kind FULL,
    dialect timeplus,
    physical_properties (
        external_type = 'kafka',
        brokers = 'kafka:29092',
        topic = 'e2e_events',
        data_format = 'JSONEachRow'
    ),
    columns (
        event_id string,
        user_id string,
        event_type string,
        amount float64,
        timestamp datetime64(3)
    ),
    description 'Kafka external stream reading from e2e_events topic'
);

-- Empty query placeholder (never executed for external streams)
SELECT
    '' AS event_id,
    '' AS user_id,
    '' AS event_type,
    0.0 AS amount,
    now64(3) AS timestamp
WHERE false;
