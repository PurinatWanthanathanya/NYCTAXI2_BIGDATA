"""
src/curate.py - Curation layer: monthly summaries, serving views, and stratified sample.
CS341 Big Data Engineering - Submission 2
"""
import glob
from pathlib import Path
import duckdb

from config import CURATED_DIR, SAMPLE_DIR, TRUSTED_DIR


def generate_curated_outputs() -> dict:
    """
    Aggregate all trusted partitions into curated monthly summary and stratified sample.
    Creates both Parquet and CSV outputs for consumption.
    """
    CURATED_DIR.mkdir(parents=True, exist_ok=True)
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    trusted_pattern = str(TRUSTED_DIR / "taxi_type=*" / "batch_month=*" / "data.parquet").replace("\\", "/")
    
    # Check if any trusted files exist
    found_files = glob.glob(str(TRUSTED_DIR / "taxi_type=*" / "batch_month=*" / "data.parquet"))
    if not found_files:
        raise FileNotFoundError("No trusted data files found to curate.")

    curated_summary_parquet = CURATED_DIR / "curated_monthly_summary.parquet"
    curated_summary_csv = CURATED_DIR / "curated_monthly_summary.csv"
    sample_parquet = SAMPLE_DIR / "curated_trips_sample.parquet"
    sample_csv = SAMPLE_DIR / "curated_trips_sample.csv"

    con = duckdb.connect()

    # 1. Create Curated Monthly Summary
    summary_sql = f"""
    CREATE OR REPLACE TABLE monthly_summary AS
    SELECT
        taxi_type,
        batch_month,
        COUNT(*) AS total_trips,
        ROUND(AVG(trip_duration_seconds), 1) AS avg_duration_seconds,
        MIN(pickup_ts) AS min_pickup_ts,
        MAX(pickup_ts) AS max_pickup_ts,
        COUNT(CASE WHEN pu_location_id IS NULL THEN 1 END) AS null_pu_locations,
        COUNT(CASE WHEN do_location_id IS NULL THEN 1 END) AS null_do_locations
    FROM read_parquet('{trusted_pattern}')
    GROUP BY taxi_type, batch_month
    ORDER BY taxi_type, batch_month;
    """
    con.execute(summary_sql)

    # Export summary to Parquet and CSV
    con.execute(
        f"COPY monthly_summary TO '{str(curated_summary_parquet).replace('\\', '/')}' (FORMAT PARQUET, COMPRESSION ZSTD);"
    )
    con.execute(
        f"COPY monthly_summary TO '{str(curated_summary_csv).replace('\\', '/')}' (HEADER, DELIMITER ',');"
    )

    # 2. Stratified Sampling (up to 250 rows per taxi_type, totaling ~1,000 rows)
    sample_sql = f"""
    CREATE OR REPLACE TABLE stratified_sample AS
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY taxi_type ORDER BY pickup_ts) AS rnk
        FROM read_parquet('{trusted_pattern}')
    )
    SELECT
        pickup_ts,
        dropoff_ts,
        pu_location_id,
        do_location_id,
        trip_duration_seconds,
        taxi_type,
        batch_month
    FROM ranked
    WHERE rnk <= 250
    ORDER BY taxi_type, pickup_ts;
    """
    con.execute(sample_sql)

    # Export sample to Parquet and CSV
    con.execute(
        f"COPY stratified_sample TO '{str(sample_parquet).replace('\\', '/')}' (FORMAT PARQUET, COMPRESSION ZSTD);"
    )
    con.execute(
        f"COPY stratified_sample TO '{str(sample_csv).replace('\\', '/')}' (HEADER, DELIMITER ',');"
    )

    # Fetch summary count
    total_summary_rows = con.execute("SELECT COUNT(*) FROM monthly_summary").fetchone()[0]
    total_sample_rows = con.execute("SELECT COUNT(*) FROM stratified_sample").fetchone()[0]
    con.close()

    return {
        "curated_summary_parquet": str(curated_summary_parquet),
        "curated_summary_csv": str(curated_summary_csv),
        "sample_parquet": str(sample_parquet),
        "sample_csv": str(sample_csv),
        "summary_rows": total_summary_rows,
        "sample_rows": total_sample_rows
    }
