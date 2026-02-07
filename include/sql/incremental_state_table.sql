CREATE TABLE IF NOT EXISTS pipeline_state (
    pipeline_name TEXT PRIMARY KEY,
    last_processed_at TIMESTAMP
);
