"""
src/transform.py - Out-of-core transformation, atomic staging, and _SUCCESS manifest.
CS341 Big Data Engineering - Submission 2
"""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import duckdb
import pyarrow.parquet as pq

from config import (
    PIPELINE_VERSION,
    SCHEMA_MAP,
    TRUSTED_DIR,
    QUARANTINE_DIR,
    compute_rules_hash,
    get_batch_date_bounds
)


def get_trusted_batch_dir(taxi_type: str, batch_month: str) -> Path:
    """Directory for trusted partitioned batch."""
    return TRUSTED_DIR / f"taxi_type={taxi_type}" / f"batch_month={batch_month}"


def get_quarantine_batch_dir(taxi_type: str, batch_month: str) -> Path:
    """Directory for quarantine partitioned batch."""
    return QUARANTINE_DIR / f"taxi_type={taxi_type}" / f"batch_month={batch_month}"


def transform_batch(raw_path: Path, taxi_type: str, batch_month: str) -> dict:
    """
    Execute streaming transformation using DuckDB.
    Writes trusted and quarantine Parquet files using atomic rename (.tmp -> final).
    Writes _SUCCESS.json containing rules_hash and reconciliation metrics.
    """
    mapping = SCHEMA_MAP[taxi_type]
    pu_col = mapping["pickup_datetime"]
    do_col = mapping["dropoff_datetime"]
    puloc_col = mapping["pu_location_id"]
    doloc_col = mapping["do_location_id"]

    start_bound, end_bound = get_batch_date_bounds(batch_month)
    clean_raw_path = str(raw_path).replace("\\", "/")

    trusted_dir = get_trusted_batch_dir(taxi_type, batch_month)
    trusted_dir.mkdir(parents=True, exist_ok=True)
    trusted_final = trusted_dir / "data.parquet"
    trusted_tmp = trusted_dir / "data.parquet.tmp"

    quarantine_dir = get_quarantine_batch_dir(taxi_type, batch_month)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine_final = quarantine_dir / "rejected.parquet"
    quarantine_tmp = quarantine_dir / "rejected.parquet.tmp"
    quarantine_csv = quarantine_dir / "rejected.csv"

    # Remove stale temp files
    if trusted_tmp.exists():
        trusted_tmp.unlink()
    if quarantine_tmp.exists():
        quarantine_tmp.unlink()

    con = duckdb.connect()

    # 1. Transform Valid Rows directly to temporary trusted Parquet
    valid_copy_sql = f"""
    COPY (
        SELECT
            "{pu_col}" AS pickup_ts,
            "{do_col}" AS dropoff_ts,
            TRY_CAST("{puloc_col}" AS BIGINT) AS pu_location_id,
            TRY_CAST("{doloc_col}" AS BIGINT) AS do_location_id,
            CAST(epoch("{do_col}") - epoch("{pu_col}") AS BIGINT) AS trip_duration_seconds,
            '{taxi_type}' AS taxi_type,
            '{batch_month}' AS batch_month
        FROM read_parquet('{clean_raw_path}')
        WHERE "{pu_col}" IS NOT NULL
          AND "{do_col}" IS NOT NULL
          AND "{do_col}" >= "{pu_col}"
          AND "{pu_col}" >= TIMESTAMP '{start_bound}'
          AND "{pu_col}" < TIMESTAMP '{end_bound}'
    ) TO '{str(trusted_tmp).replace("\\", "/")}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(valid_copy_sql)

    # 2. Extract Invalid Rows directly to temporary quarantine Parquet
    invalid_copy_sql = f"""
    COPY (
        SELECT
            *,
            CASE
                WHEN "{pu_col}" IS NULL THEN 'null_pickup'
                WHEN "{do_col}" IS NULL THEN 'null_dropoff'
                WHEN "{do_col}" < "{pu_col}" THEN 'dropoff_before_pickup'
                WHEN "{pu_col}" < TIMESTAMP '{start_bound}' OR "{pu_col}" >= TIMESTAMP '{end_bound}' THEN 'pickup_out_of_period'
                ELSE 'unknown'
            END AS reject_reason,
            '{taxi_type}' AS taxi_type,
            '{batch_month}' AS batch_month
        FROM read_parquet('{clean_raw_path}')
        WHERE "{pu_col}" IS NULL
           OR "{do_col}" IS NULL
           OR "{do_col}" < "{pu_col}"
           OR "{pu_col}" < TIMESTAMP '{start_bound}'
           OR "{pu_col}" >= TIMESTAMP '{end_bound}'
    ) TO '{str(quarantine_tmp).replace("\\", "/")}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(invalid_copy_sql)
    con.close()

    # 3. Read footer row counts to verify reconciliation
    raw_rows = pq.ParquetFile(raw_path).metadata.num_rows
    valid_written = pq.ParquetFile(trusted_tmp).metadata.num_rows
    invalid_written = pq.ParquetFile(quarantine_tmp).metadata.num_rows

    assert raw_rows == valid_written + invalid_written, (
        f"Reconciliation error for {taxi_type} {batch_month}: "
        f"raw ({raw_rows}) != valid ({valid_written}) + invalid ({invalid_written})"
    )

    # 4. Atomic file rename
    os.replace(trusted_tmp, trusted_final)
    if invalid_written > 0:
        os.replace(quarantine_tmp, quarantine_final)
        # Also export quarantine to CSV for easy inspection if small
        if invalid_written <= 5000:
            con_q = duckdb.connect()
            con_q.execute(
                f"COPY (SELECT * FROM read_parquet('{str(quarantine_final).replace('\\', '/')}')) "
                f"TO '{str(quarantine_csv).replace('\\', '/')}' (HEADER, DELIMITER ',');"
            )
            con_q.close()
    else:
        if quarantine_tmp.exists():
            quarantine_tmp.unlink()
        if quarantine_final.exists():
            quarantine_final.unlink()
        if quarantine_csv.exists():
            quarantine_csv.unlink()

    # 5. Write _SUCCESS.json
    rules_hash = compute_rules_hash(batch_month)
    success_manifest = {
        "pipeline_version": PIPELINE_VERSION,
        "taxi_type": taxi_type,
        "batch_month": batch_month,
        "rules_hash": rules_hash,
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
        "raw_rows": raw_rows,
        "valid_rows": valid_written,
        "invalid_rows": invalid_written,
        "reconciliation": "PASSED"
    }
    success_path = trusted_dir / "_SUCCESS.json"
    with open(success_path, "w", encoding="utf-8") as f:
        json.dump(success_manifest, f, indent=2)

    return success_manifest
