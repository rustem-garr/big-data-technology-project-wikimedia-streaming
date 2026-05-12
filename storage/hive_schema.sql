
CREATE DATABASE IF NOT EXISTS wikimedia_analytics;

CREATE TABLE IF NOT EXISTS wikimedia_analytics.event_counts (

  window_start TIMESTAMP,

  window_end TIMESTAMP,

  wiki STRING,

  type STRING,

  bot BOOLEAN,

  count BIGINT

)

STORED AS PARQUET;

