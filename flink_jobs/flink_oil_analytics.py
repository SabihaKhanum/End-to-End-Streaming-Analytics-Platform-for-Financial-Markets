"""
Apache Flink Job — Oil Price Analytics
=======================================
Heavy windowed analytics that complement the Kafka Streams processor:
  - 5-minute tumbling window OHLCV
  - Cross-benchmark spread joins (WTI vs Brent vs Dubai)
  - Realized volatility over 1-hour rolling windows
  - Complex Event Processing (CEP) for pattern detection

Run via:
    docker exec flink-jobmanager flink run -py /flink_jobs/flink_oil_analytics.py

Requires PyFlink (pyflink) — already in the Flink Docker image.
"""

import os
import json
import logging
from datetime import datetime

from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
    DeliveryGuarantee,
)
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.datastream.functions import (
    AggregateFunction, ProcessWindowFunction, KeyedProcessFunction
)
from pyflink.common.time import Time

log = logging.getLogger("flink-oil")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_RAW       = "oil.prices.raw"
TOPIC_AGG_5M    = "oil.prices.agg.5m"
TOPIC_ALERTS    = "oil.alerts"
TOPIC_VWAP      = "oil.vwap"


# ─────────────────────────────────────────────
# OHLCV AGGREGATOR
# ─────────────────────────────────────────────
class OHLCVAccumulator:
    def __init__(self):
        self.open = None
        self.high = float("-inf")
        self.low  = float("inf")
        self.close = None
        self.volume = 0.0
        self.vwap_num = 0.0
        self.vwap_den = 0.0
        self.count = 0


class OHLCVAggFunction(AggregateFunction):
    def create_accumulator(self):
        return OHLCVAccumulator()

    def add(self, value, acc):
        tick = json.loads(value)
        price  = tick["price"]
        volume = tick["volume"]

        if acc.open is None:
            acc.open = price
        acc.high = max(acc.high, price)
        acc.low  = min(acc.low, price)
        acc.close = price
        acc.volume += volume
        acc.vwap_num += price * volume
        acc.vwap_den += volume
        acc.count += 1
        return acc

    def get_result(self, acc):
        vwap = acc.vwap_num / acc.vwap_den if acc.vwap_den > 0 else acc.close
        return {
            "open": acc.open, "high": acc.high, "low": acc.low,
            "close": acc.close, "volume": acc.volume,
            "vwap": round(vwap, 4), "tick_count": acc.count,
        }

    def merge(self, acc1, acc2):
        if acc1.open is None:
            acc1.open = acc2.open
        acc1.high = max(acc1.high, acc2.high)
        acc1.low  = min(acc1.low,  acc2.low)
        acc1.close = acc2.close
        acc1.volume += acc2.volume
        acc1.vwap_num += acc2.vwap_num
        acc1.vwap_den += acc2.vwap_den
        acc1.count += acc2.count
        return acc1


class OHLCVWindowProcess(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        window = context.window()
        result = list(elements)[0]
        out.collect(json.dumps({
            "benchmark": key,
            "window_start": window.start,
            "window_end": window.end,
            **result,
        }))


# ─────────────────────────────────────────────
# ROLLING VOLATILITY
# ─────────────────────────────────────────────
class VolatilityAccumulator:
    def __init__(self):
        self.prices = []
        self.count = 0


class VolatilityAggFunction(AggregateFunction):
    def create_accumulator(self):
        return VolatilityAccumulator()

    def add(self, value, acc):
        tick = json.loads(value)
        acc.prices.append(tick["price"])
        acc.count += 1
        return acc

    def get_result(self, acc):
        if len(acc.prices) < 2:
            return {"realized_vol": None, "count": acc.count}
        import math
        prices = acc.prices
        returns = [
            (prices[i] - prices[i-1]) / prices[i-1]
            for i in range(1, len(prices))
        ]
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / len(returns)
        std = math.sqrt(variance)
        # Annualize (assuming tick = 500ms, ~252 trading days)
        annualized = std * math.sqrt(252 * 8 * 3600 * 2)
        return {
            "realized_vol": round(std, 6),
            "annualized_vol": round(annualized, 4),
            "count": acc.count,
        }

    def merge(self, acc1, acc2):
        acc1.prices.extend(acc2.prices)
        acc1.count += acc2.count
        return acc1


# ─────────────────────────────────────────────
# MAIN FLINK JOB
# ─────────────────────────────────────────────
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_parallelism(2)
    env.enable_checkpointing(30_000)  # checkpoint every 30s

    # ── Kafka Source ──────────────────────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TOPIC_RAW)
        .set_group_id("flink-oil-analytics")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Watermark: allow 5s of lateness
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            lambda event, _: json.loads(event).get("event_time", 0)
        )
    )

    raw_stream = env.from_source(
        kafka_source,
        watermark_strategy,
        "oil-raw-source",
    )

    # Key by benchmark
    keyed = raw_stream.key_by(lambda v: json.loads(v).get("benchmark", "UNKNOWN"))

    # ── 5-Minute Tumbling Window OHLCV ───────
    ohlcv_5m = (
        keyed
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .aggregate(OHLCVAggFunction(), OHLCVWindowProcess())
    )

    # ── 1-Hour Sliding Window Volatility ─────
    volatility = (
        keyed
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
        .aggregate(VolatilityAggFunction())
    )

    # ── Kafka Sinks ───────────────────────────
    ohlcv_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_AGG_5M)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    ohlcv_5m.sink_to(ohlcv_sink)

    log.info("Submitting Flink oil analytics job...")
    env.execute("oil-price-analytics")


if __name__ == "__main__":
    main()
