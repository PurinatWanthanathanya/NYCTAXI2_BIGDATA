"""
src/ingest.py - Atomic ingestion and metadata inspection for NYC TLC trip data.
CS341 Big Data Engineering - Submission 2
"""
import os
import time
import urllib.request
from pathlib import Path
import pyarrow.parquet as pq

from config import BASE_URL, RAW_DIR


def get_raw_filename(taxi_type: str, batch_month: str) -> str:
    """Generate canonical filename for raw NYC taxi trip parquet."""
    return f"{taxi_type}_tripdata_{batch_month}.parquet"


def download_raw_batch(taxi_type: str, batch_month: str, force: bool = False) -> Path:
    """
    Download raw parquet file from CloudFront with atomic file write.
    Writes to .tmp file first, then atomically replaces target.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    filename = get_raw_filename(taxi_type, batch_month)
    target_path = RAW_DIR / filename
    tmp_path = RAW_DIR / f"{filename}.tmp"

    if target_path.exists() and not force:
        # Validate existing file has valid footer
        try:
            pq.ParquetFile(target_path)
            return target_path
        except Exception:
            # File is corrupted, proceed to re-download
            pass

    url = f"{BASE_URL}/{filename}"
    if tmp_path.exists():
        tmp_path.unlink()

    # Streaming download with chunking
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (CS341 Big Data Pipeline)"}
    )
    with urllib.request.urlopen(req) as response, open(tmp_path, "wb") as out_file:
        chunk_size = 1024 * 1024  # 1 MB chunk
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            out_file.write(chunk)

    # Atomic rename
    os.replace(tmp_path, target_path)
    return target_path


def inspect_raw_metadata(file_path: Path) -> dict:
    """
    Inspect Parquet footer metadata without loading data rows into RAM.
    Returns num_rows, column names, and file size in MB.
    """
    parquet_file = pq.ParquetFile(file_path)
    metadata = parquet_file.metadata
    num_rows = metadata.num_rows
    columns = parquet_file.schema.names
    size_mb = file_path.stat().st_size / (1024 * 1024)

    return {
        "file_path": str(file_path),
        "num_rows": num_rows,
        "columns": columns,
        "size_mb": round(size_mb, 2)
    }
