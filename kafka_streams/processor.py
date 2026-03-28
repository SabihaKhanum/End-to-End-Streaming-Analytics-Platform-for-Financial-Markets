"""
Kafka Streams Processor
=======================
Consumes oil.prices.raw, computes:
  - 1-minute OHLCV candles
  - Volume-weighted average price (VWAP)
  - 30-tick rolling volatility (σ)
  - Spread analysis (WTI vs Brent)
  - Alert detection (spike, drop, wide spread)

Sinks to:
  - TimescaleDB (persistent storage)
  - Redis (live price cache for dashboards)
  - oil.prices.agg.1m (back to Kafka for downstream consumers)
  - oil.alerts (alert events)
"""

import os
import json
import time
import math
import logging
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import Optional

import numpy as np
import psycopg2
import psycopg2.extras
import redis
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
)
log = logging.getLogger("streams-processor")

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TIMESCALE_DSN       = os.getenv("TIMESCALE_DSN", "postgresql://oiluser:oilpass@localhost:5432/oildb")
REDIS_URL           = os.getenv("REDIS_URL", "redis://localhost:6379")

TOPIC_RAW     = "oil.prices.raw"
TOPIC_AGG_1M  = "oil.prices.agg.1m"
TOPIC_ALERTS  = "oil.alerts"
TOPIC_VWAP    = "oil.vwap"

# Alert thresholds
SPIKE_PCT_THRESHOLD    = 0.5    # % move in one tick
SPREAD_WIDE_THRESHOLD  = 5.0    # USD WTI-Brent spread
VOL_HIGH_THRESHOLD     = 0.3    # rolling σ threshold
ROLLING_WINDOW         = 30     # ticks for rolling stats


# ─────────────────────────────────────────────
# STATE: per-benchmark accumulators
# ─────────────────────────────────────────────
class BenchmarkState:
    def __init__(self, benchmark: str):
        self.benchmark = benchmark
        # 1-minute candle state
        self.candle_open: Optional[float] = None
        self.candle_high: float = float("-inf")
        self.candle_low: float  = float("inf")
        self.candle_close: Optional[float] = None
        self.candle_volume: float = 0.0
        self.candle_vwap_num: float = 0.0
        self.candle_vwap_den: float = 0.0
        self.candle_start: Optional[datetime] = None
        self.candle_tick_count: int = 0

        # Rolling window for volatility
        self.price_window: deque = deque(maxlen=ROLLING_WINDOW)
        self.prev_price: Optional[float] = None

        # All-session VWAP
        self.session_vwap_num: float = 0.0
        self.session_vwap_den: float = 0.0

        # Stats counters
        self.ticks_processed: int = 0

    def process_tick(self, price: float, volume: float, ts: datetime) -> dict:
        """Update state with new tick. Returns completed candle if minute boundary crossed."""
        completed_candle = None

        # Initialize or close 1-minute candle
        candle_minute = ts.replace(second=0, microsecond=0)
        if self.candle_start is None:
            self.candle_start = candle_minute

        if candle_minute > self.candle_start:
            # Emit completed candle
            completed_candle = {
                "time": self.candle_start,
                "benchmark": self.benchmark,
                "open": self.candle_open,
                "high": self.candle_high,
                "low": self.candle_low,
                "close": self.candle_close,
                "volume": self.candle_volume,
                "vwap": self.candle_vwap_num / self.candle_vwap_den if self.candle_vwap_den else price,
                "tick_count": self.candle_tick_count,
            }
            # Reset for new candle
            self.candle_open = price
            self.candle_high = price
            self.candle_low  = price
            self.candle_start = candle_minute
            self.candle_volume = 0.0
            self.candle_vwap_num = 0.0
            self.candle_vwap_den = 0.0
            self.candle_tick_count = 0
        else:
            if self.candle_open is None:
                self.candle_open = price

        # Update running candle
        self.candle_high = max(self.candle_high, price)
        self.candle_low  = min(self.candle_low, price)
        self.candle_close = price
        self.candle_volume += volume
        self.candle_vwap_num += price * volume
        self.candle_vwap_den += volume
        self.candle_tick_count += 1

        # Session VWAP
        self.session_vwap_num += price * volume
        self.session_vwap_den += volume

        # Rolling price window
        self.price_window.append(price)

        self.prev_price = price
        self.ticks_processed += 1

        return completed_candle

    @property
    def rolling_vol(self) -> Optional[float]:
        if len(self.price_window) < 5:
            return None
        arr = np.array(self.price_window)
        returns = np.diff(arr) / arr[:-1]
        return float(np.std(returns))

    @property
    def session_vwap(self) -> Optional[float]:
        if self.session_vwap_den == 0:
            return None
        return self.session_vwap_num / self.session_vwap_den


# ─────────────────────────────────────────────
# TIMESCALEDB SINK
# ─────────────────────────────────────────────
class TimeScaleSink:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = None
        self._connect()
        self._batch_ticks: list = []
        self._batch_candles: list = []
        self._batch_alerts: list = []
        self._batch_vol: list = []
        self._last_flush = time.monotonic()
        self.FLUSH_INTERVAL = 2.0   # seconds
        self.BATCH_SIZE = 200

    def _connect(self):
        for attempt in range(20):
            try:
                self.conn = psycopg2.connect(self.dsn)
                self.conn.autocommit = False
                log.info("TimescaleDB connected.")
                return
            except Exception as e:
                log.warning("TimescaleDB connect attempt %d: %s", attempt + 1, e)
                time.sleep(3)
        raise RuntimeError("Could not connect to TimescaleDB")

    def _ensure_connection(self):
        try:
            self.conn.cursor().execute("SELECT 1")
        except Exception:
            log.warning("Reconnecting to TimescaleDB...")
            self._connect()

    def queue_tick(self, tick: dict):
        self._batch_ticks.append(tick)
        self._maybe_flush()

    def queue_candle(self, candle: dict):
        self._batch_candles.append(candle)
        self._maybe_flush()

    def queue_alert(self, alert: dict):
        self._batch_alerts.append(alert)
        self._maybe_flush()

    def queue_vol(self, vol: dict):
        self._batch_vol.append(vol)
        self._maybe_flush()

    def _maybe_flush(self):
        now = time.monotonic()
        total = len(self._batch_ticks) + len(self._batch_candles)
        if total >= self.BATCH_SIZE or (now - self._last_flush) >= self.FLUSH_INTERVAL:
            self.flush()

    def flush(self):
        self._ensure_connection()
        try:
            with self.conn.cursor() as cur:
                if self._batch_ticks:
                    psycopg2.extras.execute_values(cur, """
                        INSERT INTO oil_ticks
                            (time, benchmark, exchange, price, volume, bid, ask, source)
                        VALUES %s
                        ON CONFLICT DO NOTHING
                    """, [(
                        t["event_time_dt"], t["benchmark"], t["exchange"],
                        t["price"], t["volume"], t.get("bid"), t.get("ask"),
                        t.get("source", "simulator")
                    ) for t in self._batch_ticks])

                if self._batch_candles:
                    psycopg2.extras.execute_values(cur, """
                        INSERT INTO oil_ohlcv_1m
                            (time, benchmark, open, high, low, close, volume, vwap, tick_count)
                        VALUES %s
                        ON CONFLICT DO NOTHING
                    """, [(
                        c["time"], c["benchmark"], c["open"], c["high"],
                        c["low"], c["close"], c["volume"], c.get("vwap"), c.get("tick_count")
                    ) for c in self._batch_candles])

                if self._batch_alerts:
                    psycopg2.extras.execute_values(cur, """
                        INSERT INTO oil_alerts
                            (time, benchmark, alert_type, price, delta, delta_pct, severity, message)
                        VALUES %s
                    """, [(
                        a["time"], a["benchmark"], a["alert_type"],
                        a["price"], a.get("delta"), a.get("delta_pct"),
                        a.get("severity", "MEDIUM"), a.get("message")
                    ) for a in self._batch_alerts])

                if self._batch_vol:
                    psycopg2.extras.execute_values(cur, """
                        INSERT INTO oil_volatility
                            (time, benchmark, rolling_std_30)
                        VALUES %s
                        ON CONFLICT DO NOTHING
                    """, [(v["time"], v["benchmark"], v["rolling_std_30"]) for v in self._batch_vol])

            self.conn.commit()
            n = len(self._batch_ticks)
            if n > 0:
                log.debug("Flushed %d ticks, %d candles, %d alerts to TimescaleDB",
                          n, len(self._batch_candles), len(self._batch_alerts))

            self._batch_ticks.clear()
            self._batch_candles.clear()
            self._batch_alerts.clear()
            self._batch_vol.clear()
            self._last_flush = time.monotonic()

        except Exception as e:
            log.error("TimescaleDB flush error: %s", e)
            self.conn.rollback()


# ─────────────────────────────────────────────
# REDIS SINK
# ─────────────────────────────────────────────
class RedisSink:
    def __init__(self, url: str):
        self.client = redis.from_url(url, decode_responses=True)
        log.info("Redis connected.")

    def update_live_price(self, benchmark: str, price: float, vwap: Optional[float],
                          vol: Optional[float], ts: datetime):
        pipe = self.client.pipeline()
        key = f"oil:live:{benchmark}"
        pipe.hset(key, mapping={
            "price": round(price, 3),
            "vwap": round(vwap, 3) if vwap else "",
            "vol": round(vol, 6) if vol else "",
            "ts": ts.isoformat(),
        })
        pipe.expire(key, 60)

        # Add to sorted set for time-series (score = unix ts, member = price)
        ts_key = f"oil:ts:{benchmark}"
        pipe.zadd(ts_key, {f"{ts.timestamp()}:{price}" : ts.timestamp()})
        pipe.zremrangebyscore(ts_key, 0, ts.timestamp() - 3600)  # keep 1h

        # Pub/Sub for live dashboard push
        pipe.publish(f"oil:tick:{benchmark}", json.dumps({
            "benchmark": benchmark,
            "price": round(price, 3),
            "vwap": round(vwap, 3) if vwap else None,
            "ts": ts.isoformat(),
        }))

        pipe.execute()

    def publish_alert(self, alert: dict):
        self.client.publish("oil:alerts", json.dumps(alert))
        self.client.lpush("oil:alerts:history", json.dumps(alert))
        self.client.ltrim("oil:alerts:history", 0, 99)  # keep last 100


# ─────────────────────────────────────────────
# ALERT ENGINE
# ─────────────────────────────────────────────
class AlertEngine:
    def __init__(self, kafka_producer: Producer, redis_sink: RedisSink,
                 ts_sink: TimeScaleSink):
        self.producer = kafka_producer
        self.redis = redis_sink
        self.ts = ts_sink
        self._prev_prices: dict = {}

    def check(self, benchmark: str, price: float, vol: Optional[float],
              ts: datetime, wti_price: Optional[float], brent_price: Optional[float]):
        prev = self._prev_prices.get(benchmark)
        self._prev_prices[benchmark] = price

        if prev is None:
            return

        delta = price - prev
        delta_pct = (delta / prev) * 100

        # --- Spike / Drop alert ---
        if abs(delta_pct) >= SPIKE_PCT_THRESHOLD:
            alert_type = "SPIKE" if delta > 0 else "DROP"
            severity = "CRITICAL" if abs(delta_pct) > 2.0 else \
                       "HIGH" if abs(delta_pct) > 1.0 else "MEDIUM"
            self._emit_alert(benchmark, alert_type, price, delta, delta_pct,
                             severity, ts,
                             f"{benchmark} {alert_type.lower()}: {delta_pct:+.2f}%")

        # --- Wide spread (WTI vs Brent) ---
        if benchmark == "WTI" and brent_price:
            spread = abs(price - brent_price)
            if spread > SPREAD_WIDE_THRESHOLD:
                self._emit_alert("WTI", "SPREAD_WIDE", price, spread, None,
                                 "HIGH", ts,
                                 f"WTI/Brent spread unusually wide: ${spread:.2f}")

        # --- High volatility ---
        if vol and vol > VOL_HIGH_THRESHOLD:
            self._emit_alert(benchmark, "VOLATILITY", price, vol, None,
                             "MEDIUM", ts,
                             f"{benchmark} rolling σ elevated: {vol:.4f}")

    def _emit_alert(self, benchmark, alert_type, price, delta, delta_pct,
                    severity, ts, message):
        alert = {
            "time": ts,
            "benchmark": benchmark,
            "alert_type": alert_type,
            "price": price,
            "delta": delta,
            "delta_pct": delta_pct,
            "severity": severity,
            "message": message,
        }
        log.warning("ALERT [%s] %s — %s", severity, benchmark, message)
        self.ts.queue_alert(alert)
        self.redis.publish_alert({**alert, "time": ts.isoformat()})

        # Publish to Kafka alerts topic
        try:
            self.producer.produce(
                TOPIC_ALERTS,
                key=benchmark.encode(),
                value=json.dumps({**alert, "time": ts.isoformat()}).encode(),
            )
        except Exception as e:
            log.error("Alert produce error: %s", e)


# ─────────────────────────────────────────────
# MAIN PROCESSOR
# ─────────────────────────────────────────────
def main():
    log.info("Starting streams processor | bootstrap=%s", KAFKA_BOOTSTRAP)

    # Kafka consumer
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "oil-streams-processor",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 30000,
    })

    # Kafka producer (for agg output)
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 10,
    })

    # Avro deserializer
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(sr)

    # Sinks
    ts_sink    = TimeScaleSink(TIMESCALE_DSN)
    redis_sink = RedisSink(REDIS_URL)

    # Per-benchmark state
    states: dict[str, BenchmarkState] = {}

    # Alert engine
    alert_engine = AlertEngine(producer, redis_sink, ts_sink)

    consumer.subscribe([TOPIC_RAW])
    log.info("Subscribed to %s", TOPIC_RAW)

    msg_count = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                ts_sink.flush()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Consumer error: %s", msg.error())
                continue

            try:
                tick = deserializer(
                    msg.value(),
                    SerializationContext(TOPIC_RAW, MessageField.VALUE),
                )
            except Exception as e:
                log.error("Deserialization error: %s", e)
                consumer.commit(msg)
                continue

            benchmark = tick["benchmark"]
            price     = tick["price"]
            volume    = tick["volume"]
            ts_ms     = tick["event_time"]
            ts        = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

            tick["event_time_dt"] = ts

            # Get or create benchmark state
            if benchmark not in states:
                states[benchmark] = BenchmarkState(benchmark)

            state = states[benchmark]
            completed_candle = state.process_tick(price, volume, ts)

            # Write tick to TimescaleDB
            ts_sink.queue_tick(tick)

            # Write completed candle
            if completed_candle:
                ts_sink.queue_candle(completed_candle)
                producer.produce(
                    TOPIC_AGG_1M,
                    key=benchmark.encode(),
                    value=json.dumps({
                        **completed_candle,
                        "time": completed_candle["time"].isoformat(),
                    }).encode(),
                )
                log.info("Candle | %s O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f",
                         benchmark,
                         completed_candle["open"], completed_candle["high"],
                         completed_candle["low"], completed_candle["close"],
                         completed_candle["volume"])

            # Update Redis live cache
            redis_sink.update_live_price(
                benchmark, price, state.session_vwap, state.rolling_vol, ts
            )

            # Write volatility metric
            if state.rolling_vol and msg_count % 10 == 0:
                ts_sink.queue_vol({
                    "time": ts,
                    "benchmark": benchmark,
                    "rolling_std_30": state.rolling_vol,
                })

            # Run alert checks
            alert_engine.check(
                benchmark, price, state.rolling_vol, ts,
                states.get("WTI", None) and states["WTI"].prev_price,
                states.get("BRENT", None) and states.get("BRENT", BenchmarkState("BRENT")).prev_price,
            )

            consumer.commit(msg)
            msg_count += 1

            if msg_count % 500 == 0:
                log.info("Processed %d messages | states: %s",
                         msg_count,
                         {k: f"{v.prev_price:.2f}" for k, v in states.items() if v.prev_price})
                producer.poll(0)

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        ts_sink.flush()
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
