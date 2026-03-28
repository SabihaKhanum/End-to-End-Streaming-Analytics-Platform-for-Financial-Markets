"""Apache Superset configuration for Oil Price Analytics"""

import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "a-very-secret-key-change-in-prod")

# TimescaleDB connection (same DB as the pipeline)
SQLALCHEMY_DATABASE_URI = os.getenv(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://oiluser:oilpass@timescaledb:5432/oildb"
)

# Enable async queries for large datasets
SUPERSET_ASYNC_QUERIES_REDIS_BACKEND = "redis://redis:6379/0"
RESULTS_BACKEND_USE_MSGPACK = True

# Allow all origins in dev (restrict in prod)
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False

# Cache config using Redis
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": "redis://redis:6379/1",
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 60,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_URL": "redis://redis:6379/2",
}

# Enable TimescaleDB-specific features
PREVENT_UNSAFE_DB_CONNECTIONS = False

# Row limit for dashboard queries
ROW_LIMIT = 100000

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERTS_ATTACH_REPORTS": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}
