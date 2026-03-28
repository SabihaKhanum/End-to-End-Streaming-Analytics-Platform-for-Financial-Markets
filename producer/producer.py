"""
Oil Price Producer
==================
Simulates real-time oil price tick data for WTI, Brent, and Dubai crude,
serializes with Avro, and publishes to Kafka with Schema Registry.

In production, replace the SimulatedFeed with real WebSocket/REST feeds:
  - CME DataMine (WTI)      : wss://api.cmegroup.com/...
  - ICE Connect (Brent)     : wss://api.theice.com/...
  - ADNOC / S&P Platts (Dubai)
  - Refinitiv Eikon API
"""

import os
import json
import time
import math
import random
import logging
import threading
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
)
log = logging.getLogger("oil-producer")

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY   = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC_RAW         = "oil.prices.raw"
TICK_INTERVAL_MS  = int(os.getenv("TICK_INTERVAL_MS", "500"))

# Load Avro schema
with open("/app/oil_tick.avsc") as f:
    SCHEMA_STR = f.read()


# ─────────────────────────────────────────────
# MARKET SIMULATOR
# Uses Geometric Brownian Motion (GBM) for realistic price simulation
# dS = μ·S·dt + σ·S·dW
# ─────────────────────────────────────────────
@dataclass
class MarketState:
    benchmark: str
    exchange: str
    region: str
    price: float
    mu: float = 0.0          # drift (annualized)
    sigma: float = 0.25      # volatility (annualized)
    spread_pct: float = 0.001
    volume_mean: float = 5000.0
    open_interest: float = 1_200_000.0
    _rng: np.random.Generator = field(default_factory=lambda: np.random.default_rng())

    def next_tick(self, dt: float = 0.001) -> dict:
        """Generate next price using GBM with mean-reversion component."""
        # GBM step
        dW = self._rng.normal(0, math.sqrt(dt))
        drift = (self.mu - 0.5 * self.sigma ** 2) * dt
        diffusion = self.sigma * dW
        self.price *= math.exp(drift + diffusion)

        # Clamp to realistic range
        self.price = max(30.0, min(200.0, self.price))

        half_spread = self.price * self.spread_pct / 2
        volume = max(100, self._rng.lognormal(
            mean=math.log(self.volume_mean), sigma=0.4
        ))
        self.open_interest += self._rng.normal(0, 1000)
        self.open_interest = max(500_000, self.open_interest)

        return {
            "event_time": int(datetime.now(timezone.utc).timestamp() * 1000),
            "benchmark": self.benchmark,
            "exchange": self.exchange,
            "price": round(self.price, 3),
            "volume": round(float(volume), 1),
            "bid": round(self.price - half_spread, 3),
            "ask": round(self.price + half_spread, 3),
            "open_interest": round(self.open_interest, 0),
            "source": "simulator",
            "region": self.region,
        }


# Initial prices (approximate real-world values)
MARKETS = [
    MarketState("WTI",   "NYMEX",  "NORTH_AMERICA", price=78.42,  sigma=0.28),
    MarketState("BRENT", "ICE",    "EUROPE",         price=82.15,  sigma=0.25),
    MarketState("DUBAI", "ADNOC",  "MIDDLE_EAST",    price=80.90,  sigma=0.22),
]


# ─────────────────────────────────────────────
# KAFKA SETUP
# ─────────────────────────────────────────────
def build_producer() -> tuple:
    """Build Kafka producer with Avro serializer."""
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY})

    avro_serializer = AvroSerializer(
        sr_client,
        SCHEMA_STR,
        lambda obj, ctx: obj,  # already a dict
    )
    string_serializer = StringSerializer("utf_8")

    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 5,
        "batch.size": 65536,
        "retries": 5,
        "retry.backoff.ms": 100,
    }

    producer = Producer(producer_conf)
    return producer, avro_serializer, string_serializer


def delivery_callback(err, msg):
    if err:
        log.error("Delivery failed | topic=%s partition=%d error=%s",
                  msg.topic(), msg.partition(), err)
    else:
        log.debug("Delivered | topic=%s partition=%d offset=%d",
                  msg.topic(), msg.partition(), msg.offset())


# ─────────────────────────────────────────────
# MARKET EVENT INJECTOR
# Randomly injects geopolitical / macro events
# ─────────────────────────────────────────────
class EventInjector:
    """Periodically injects simulated market events (OPEC cuts, sanctions, etc.)."""

    EVENTS = [
        {"name": "OPEC+ production cut",     "sigma_mult": 2.5, "mu_delta": +0.02},
        {"name": "US strategic reserve release", "sigma_mult": 2.0, "mu_delta": -0.015},
        {"name": "Iran sanctions escalation", "sigma_mult": 3.0, "mu_delta": +0.03},
        {"name": "Libya supply disruption",  "sigma_mult": 2.0, "mu_delta": +0.02},
        {"name": "Demand slowdown signal",   "sigma_mult": 1.8, "mu_delta": -0.02},
        {"name": "Dollar strengthening",     "sigma_mult": 1.5, "mu_delta": -0.01},
        {"name": "Hurricane Gulf shutdown",  "sigma_mult": 2.2, "mu_delta": +0.025},
    ]

    def __init__(self, markets: list):
        self.markets = markets
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self):
        while True:
            time.sleep(random.uniform(30, 120))  # event every 30–120s
            event = random.choice(self.EVENTS)
            duration = random.uniform(5, 20)     # seconds of elevated vol
            log.info("EVENT: %s (duration=%.0fs)", event["name"], duration)

            for m in self.markets:
                orig_sigma = m.sigma
                m.sigma *= event["sigma_mult"]
                m.mu += event["mu_delta"]

            time.sleep(duration)

            for m in self.markets:
                m.sigma = orig_sigma
                m.mu = 0.0


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────
def main():
    log.info("Starting oil price producer | bootstrap=%s", KAFKA_BOOTSTRAP)
    log.info("Connecting to Schema Registry at %s ...", SCHEMA_REGISTRY)

    # Wait for Schema Registry
    for attempt in range(20):
        try:
            r = requests.get(f"{SCHEMA_REGISTRY}/subjects", timeout=3)
            if r.status_code == 200:
                log.info("Schema Registry ready.")
                break
        except Exception:
            pass
        log.info("Waiting for Schema Registry... (%d/20)", attempt + 1)
        time.sleep(3)

    producer, avro_serializer, string_serializer = build_producer()

    # Start event injector
    EventInjector(MARKETS)

    tick_count = 0
    interval_s = TICK_INTERVAL_MS / 1000.0

    log.info("Publishing ticks every %.0fms to topic '%s'", TICK_INTERVAL_MS, TOPIC_RAW)

    while True:
        start = time.monotonic()

        for market in MARKETS:
            tick = market.next_tick(dt=interval_s / (252 * 8 * 3600))

            # Partition key = benchmark name (ensures ordering per benchmark)
            key = string_serializer(tick["benchmark"])

            try:
                producer.produce(
                    topic=TOPIC_RAW,
                    key=key,
                    value=avro_serializer(
                        tick,
                        SerializationContext(TOPIC_RAW, MessageField.VALUE),
                    ),
                    on_delivery=delivery_callback,
                )
            except Exception as e:
                log.error("Produce error: %s", e)

        producer.poll(0)
        tick_count += len(MARKETS)

        if tick_count % 100 == 0:
            log.info("Published %d ticks | WTI=%.2f Brent=%.2f Dubai=%.2f",
                     tick_count,
                     MARKETS[0].price, MARKETS[1].price, MARKETS[2].price)

        # Sleep remainder of interval
        elapsed = time.monotonic() - start
        sleep_time = max(0.0, interval_s - elapsed)
        time.sleep(sleep_time)

    producer.flush()


if __name__ == "__main__":
    main()
