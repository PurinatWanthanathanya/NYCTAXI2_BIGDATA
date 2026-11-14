"""
src/validate.py - Schema contract validation and data quality audit.
CS341 Big Data Engineering - Submission 2
"""
from pathlib import Path
import duckdb
import pyarrow.parquet as pq

from config import SCHEMA_MAP, get_batch_date_bounds


def validate_schema_contract(file_path: Path, taxi_type: str) -> tuple[bool, list[str]]:
    """
    Validate that raw Parquet file contains required canonical columns.
    Returns (is_valid, missing_columns).
    """
    required_mapping = SCHEMA_MAP[taxi_type]
    parquet_file = pq.ParquetFile(file_path)
    file_cols = set(parquet_file.schema.names)

    missing = [
        raw_col for raw_col in required_mapping.values()
        if raw_col not in file_cols
    ]
    return (len(missing) == 0, missing)


def audit_batch_data_quality(file_path: Path, taxi_type: str, batch_month: str) -> dict:
    """
    Run data quality check via DuckDB out-of-core query.
    Calculates:
      - Overlapping rule failure counts
      - Mutually exclusive prioritized rejection counts
      - Null location counts (FHV nullable checks)
      - Duration outliers (0s, >24h)
    """
    mapping = SCHEMA_MAP[taxi_type]
    pu_col = mapping["pickup_datetime"]
    do_col = mapping["dropoff_datetime"]
    puloc_col = mapping["pu_location_id"]
    doloc_col = mapping["do_location_id"]

    start_bound, end_bound = get_batch_date_bounds(batch_month)

    # Use forward slashes for DuckDB path compatibility
    clean_path = str(file_path).replace("\\", "/")

    con = duckdb.connect()
    audit_sql = f"""
    SELECT
        COUNT(*) AS total_rows,
        -- Overlapping counts (rows violating each individual rule)
        COUNT(CASE WHEN "{pu_col}" IS NULL THEN 1 END) AS count_overlap_null_pu,
        COUNT(CASE WHEN "{do_col}" IS NULL THEN 1 END) AS count_overlap_null_do,
        COUNT(CASE WHEN "{do_col}" < "{pu_col}" THEN 1 END) AS count_overlap_dropoff_before_pu,
        COUNT(CASE WHEN "{pu_col}" < TIMESTAMP '{start_bound}' OR "{pu_col}" >= TIMESTAMP '{end_bound}' THEN 1 END) AS count_overlap_out_of_period,

        -- Mutually exclusive counts based on priority order
        COUNT(CASE
            WHEN "{pu_col}" IS NULL THEN 1
            ELSE NULL
        END) AS prio_null_pickup,

        COUNT(CASE
            WHEN "{pu_col}" IS NOT NULL AND "{do_col}" IS NULL THEN 1
            ELSE NULL
        END) AS prio_null_dropoff,

        COUNT(CASE
            WHEN "{pu_col}" IS NOT NULL AND "{do_col}" IS NOT NULL AND "{do_col}" < "{pu_col}" THEN 1
            ELSE NULL
        END) AS prio_dropoff_before_pickup,

        COUNT(CASE
            WHEN "{pu_col}" IS NOT NULL AND "{do_col}" IS NOT NULL AND "{do_col}" >= "{pu_col}"
                 AND ("{pu_col}" < TIMESTAMP '{start_bound}' OR "{pu_col}" >= TIMESTAMP '{end_bound}') THEN 1
            ELSE NULL
        END) AS prio_pickup_out_of_period,

        -- Location nullability metrics
        COUNT(CASE WHEN "{puloc_col}" IS NULL THEN 1 END) AS count_null_puloc,
        COUNT(CASE WHEN "{doloc_col}" IS NULL THEN 1 END) AS count_null_doloc,

        -- Sensitivity metrics (Outlier != Error)
        COUNT(CASE
            WHEN "{pu_col}" IS NOT NULL AND "{do_col}" IS NOT NULL
                 AND "{do_col}" >= "{pu_col}"
                 AND ("{pu_col}" >= TIMESTAMP '{start_bound}' AND "{pu_col}" < TIMESTAMP '{end_bound}')
                 AND "{do_col}" = "{pu_col}"
            THEN 1 END) AS count_zero_duration,

        COUNT(CASE
            WHEN "{pu_col}" IS NOT NULL AND "{do_col}" IS NOT NULL
                 AND "{do_col}" >= "{pu_col}"
                 AND ("{pu_col}" >= TIMESTAMP '{start_bound}' AND "{pu_col}" < TIMESTAMP '{end_bound}')
                 AND epoch("{do_col}") - epoch("{pu_col}") > 86400
            THEN 1 END) AS count_gt_24h_duration

    FROM read_parquet('{clean_path}')
    """

    res = con.execute(audit_sql).fetchone()
    con.close()

    total_rows = res[0]
    prio_null_pu = res[5]
    prio_null_do = res[6]
    prio_do_before_pu = res[7]
    prio_out_period = res[8]

    total_invalid = prio_null_pu + prio_null_do + prio_do_before_pu + prio_out_period
    valid_rows = total_rows - total_invalid

    return {
        "taxi_type": taxi_type,
        "batch_month": batch_month,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "total_invalid": total_invalid,
        "prio_null_pickup": prio_null_pu,
        "prio_null_dropoff": prio_null_do,
        "prio_dropoff_before_pickup": prio_do_before_pu,
        "prio_pickup_out_of_period": prio_out_period,
        "overlap_null_pickup": res[1],
        "overlap_null_dropoff": res[2],
        "overlap_dropoff_before_pickup": res[3],
        "overlap_pickup_out_of_period": res[4],
        "null_pu_location": res[9],
        "null_do_location": res[10],
        "zero_duration": res[11],
        "gt_24h_duration": res[12]
    }
