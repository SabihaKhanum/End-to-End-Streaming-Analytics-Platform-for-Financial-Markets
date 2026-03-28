#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
sleep 5

BROKER="kafka:9092"

create_topic() {
  local name=$1
  local partitions=$2
  local retention_ms=$3

  kafka-topics --bootstrap-server $BROKER \
    --create --if-not-exists \
    --topic $name \
    --partitions $partitions \
    --replication-factor 1 \
    --config retention.ms=$retention_ms \
    --config cleanup.policy=delete

  echo "Created topic: $name (partitions=$partitions)"
}

# Raw tick data — partitioned by benchmark (WTI=0, Brent=1, Dubai=2)
create_topic "oil.prices.raw"       3  86400000   # 1 day retention

# Aggregated OHLCV candles (1s, 1m, 5m)
create_topic "oil.prices.agg.1s"    3  3600000    # 1 hour
create_topic "oil.prices.agg.1m"    3  604800000  # 7 days
create_topic "oil.prices.agg.5m"    3  2592000000 # 30 days

# Alerts from CEP engine
create_topic "oil.alerts"           1  604800000

# Sentiment signals
create_topic "oil.sentiment"        1  86400000

# VWAP output stream
create_topic "oil.vwap"             3  3600000

echo "All topics created successfully."
kafka-topics --bootstrap-server $BROKER --list
