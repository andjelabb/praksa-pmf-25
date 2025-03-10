CREATE SOURCE IF NOT EXISTS user_events_source (
    timestamp TIMESTAMP,
    user_id INT,
    action VARCHAR,
    bank VARCHAR,
    transaction_type VARCHAR,
    amount REAL,
    message_key VARCHAR
)
WITH (
   connector='kafka',
   topic='user_events',
   properties.bootstrap.server='redpanda-0:9092'
) FORMAT PLAIN ENCODE JSON;

