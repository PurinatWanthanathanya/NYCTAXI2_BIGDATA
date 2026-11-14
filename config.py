"""
config.py - Centralized configuration, schema mappings, and rules for NYC Taxi Pipeline.
CS341 Big Data Engineering - Submission 2
"""
import hashlib
import json
from pathlib import Path

# Pipeline metadata
PIPELINE_VERSION = "2.0.0"

# Workspace directories
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
TRUSTED_DIR = DATA_DIR / "trusted"
QUARANTINE_DIR = DATA_DIR / "quarantine"
CURATED_DIR = DATA_DIR / "curated"
SAMPLE_DIR = DATA_DIR / "sample"
LOGS_DIR = BASE_DIR / "logs"
OUTPUTS_DIR = BASE_DIR / "outputs"

# Remote source
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Scope
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]
H1_MONTHS = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]

# Source Schema Mapping -> Canonical Schema
SCHEMA_MAP = {
    "yellow": {
        "pickup_datetime": "tpep_pickup_datetime",
        "dropoff_datetime": "tpep_dropoff_datetime",
        "pu_location_id": "PULocationID",
        "do_location_id": "DOLocationID"
    },
    "green": {
        "pickup_datetime": "lpep_pickup_datetime",
        "dropoff_datetime": "lpep_dropoff_datetime",
        "pu_location_id": "PULocationID",
        "do_location_id": "DOLocationID"
    },
    "fhv": {
        "pickup_datetime": "pickup_datetime",
        "dropoff_datetime": "dropOff_datetime",  # capital Off in FHV
        "pu_location_id": "PUlocationID",       # mixed case in FHV
        "do_location_id": "DOlocationID"        # mixed case in FHV
    },
    "fhvhv": {
        "pickup_datetime": "pickup_datetime",
        "dropoff_datetime": "dropoff_datetime",
        "pu_location_id": "PULocationID",
        "do_location_id": "DOLocationID"
    }
}

# Canonical columns retained in trusted zone
TRUSTED_COLUMNS = [
    "pickup_ts",
    "dropoff_ts",
    "pu_location_id",
    "do_location_id",
    "trip_duration_seconds",
    "taxi_type",
    "batch_month"
]

# Mutually exclusive priority order for quarantine categorization
REJECT_PRIORITY_ORDER = [
    "null_pickup",
    "null_dropoff",
    "dropoff_before_pickup",
    "pickup_out_of_period"
]

# Baseline expected numbers from Submission 1 for automated regression check
BASELINE_H1_EXPECTED = {
    "total_raw": 149655796,
    "total_valid": 149655451,
    "total_invalid": 345,
    "yellow_valid": 20331754,
    "green_valid": 339801,
    "fhv_valid": 8119228,
    "fhvhv_valid": 120864668,
}


def get_batch_date_bounds(batch_month: str) -> tuple[str, str]:
    """
    Returns (start_timestamp_inclusive, end_timestamp_exclusive) for a batch.
    For H1 2024 batches (2024-01 through 2024-06), bounds match S1 definition:
    pickup must be within 2024-01-01 00:00:00 and 2024-07-01 00:00:00.
    For batches after H1, bounds are the calendar month of that batch.
    """
    if batch_month in H1_MONTHS:
        return "2024-01-01 00:00:00", "2024-07-01 00:00:00"
    
    year, month = map(int, batch_month.split("-"))
    start_dt = f"{year:04d}-{month:02d}-01 00:00:00"
    if month == 12:
        end_dt = f"{year+1:04d}-01-01 00:00:00"
    else:
        end_dt = f"{year:04d}-{month+1:02d}-01 00:00:00"
    return start_dt, end_dt


def compute_rules_hash(batch_month: str) -> str:
    """
    Compute SHA-256 fingerprint of pipeline version, schemas, columns, rules,
    and batch date bounds. Stored in _SUCCESS.json to invalidate stale outputs.
    """
    bounds = get_batch_date_bounds(batch_month)
    payload = {
        "pipeline_version": PIPELINE_VERSION,
        "schema_map": SCHEMA_MAP,
        "trusted_columns": TRUSTED_COLUMNS,
        "reject_priority_order": REJECT_PRIORITY_ORDER,
        "batch_date_bounds": bounds
    }
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
