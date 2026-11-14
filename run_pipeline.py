"""
run_pipeline.py - Orchestrator CLI for NYC Taxi Big Data Pipeline.
CS341 Big Data Engineering - Submission 2
"""
import argparse
from datetime import datetime, timezone
import glob
import json
import os
from pathlib import Path
import time
import uuid

import pandas as pd

from config import (
    BASE_DIR,
    DATA_DIR,
    H1_MONTHS,
    LOGS_DIR,
    OUTPUTS_DIR,
    PIPELINE_VERSION,
    TAXI_TYPES,
    compute_rules_hash
)
from src.analyze import (
    answer_business_question,
    generate_pipeline_diagram,
    generate_visualizations,
    run_sensitivity_analysis,
    verify_h1_regression
)
from src.curate import generate_curated_outputs
from src.ingest import download_raw_batch, inspect_raw_metadata
from src.transform import get_trusted_batch_dir, transform_batch
from src.validate import audit_batch_data_quality, validate_schema_contract


def cleanup_stale_temp_files():
    """Remove any orphaned .tmp files left over from interrupted runs."""
    pattern = str(DATA_DIR / "**" / "*.tmp")
    for tmp_file in glob.glob(pattern, recursive=True):
        try:
            os.remove(tmp_file)
            print(f"[CLEANUP] Removed stale temporary file: {tmp_file}")
        except Exception as e:
            print(f"[CLEANUP WARNING] Failed to remove {tmp_file}: {e}")


def update_dq_report(dq_records: list[dict]):
    """Upsert data quality metrics into logs/dq_report_by_batch.csv."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    dq_csv = LOGS_DIR / "dq_report_by_batch.csv"

    new_df = pd.DataFrame(dq_records)
    if dq_csv.exists():
        existing_df = pd.read_csv(dq_csv)
        # Deduplicate on (taxi_type, batch_month), keeping newest
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["taxi_type", "batch_month"], keep="last")
        combined.to_csv(dq_csv, index=False)
    else:
        new_df.to_csv(dq_csv, index=False)


def append_pipeline_run_log(run_records: list[dict]):
    """Append execution log entries to logs/pipeline_run_log.csv."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_csv = LOGS_DIR / "pipeline_run_log.csv"

    new_df = pd.DataFrame(run_records)
    if log_csv.exists():
        new_df.to_csv(log_csv, mode="a", header=False, index=False)
    else:
        new_df.to_csv(log_csv, index=False)


def main():
    parser = argparse.ArgumentParser(description="NYC Taxi Big Data Pipeline (CS341 Submission 2)")
    parser.add_argument("--types", nargs="+", default=TAXI_TYPES, choices=TAXI_TYPES, help="Taxi types to process")
    parser.add_argument("--months", nargs="+", default=H1_MONTHS, help="Months to process (YYYY-MM)")
    parser.add_argument("--mode", choices=["skip", "force"], default="skip", help="Processing mode: skip already processed or force re-process")
    parser.add_argument("--skip-download", action="store_true", help="Skip remote download and use local raw files")
    parser.add_argument("--skip-sensitivity", action="store_true", help="Skip sensitivity analysis check")
    args = parser.parse_args()

    run_id = str(uuid.uuid4())[:8]
    start_utc = datetime.now(timezone.utc)
    t_start = time.time()

    print("=" * 80)
    print(f"NYC TAXI BIG DATA PIPELINE v{PIPELINE_VERSION}")
    print(f"Run ID: {run_id} | Mode: {args.mode.upper()} | Start: {start_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Target Types: {args.types}")
    print(f"Target Months: {args.months}")
    print("=" * 80)

    # 1. Clean stale temporary files
    cleanup_stale_temp_files()

    dq_records = []
    run_records = []
    processed_count = 0
    skipped_count = 0

    # 2. Process each batch
    for taxi_type in args.types:
        for batch_month in args.months:
            trusted_dir = get_trusted_batch_dir(taxi_type, batch_month)
            success_path = trusted_dir / "_SUCCESS.json"
            trusted_file = trusted_dir / "data.parquet"

            current_rules_hash = compute_rules_hash(batch_month)

            batch_status = "SUCCESS"

            # Check skip condition
            if args.mode == "skip" and success_path.exists() and trusted_file.exists():
                try:
                    with open(success_path, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                    if meta.get("rules_hash") == current_rules_hash:
                        print(f"[-] SKIPPED: {taxi_type} {batch_month} (Already processed & valid rules hash)")
                        skipped_count += 1
                        run_records.append({
                            "run_id": run_id,
                            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                            "taxi_type": taxi_type,
                            "batch_month": batch_month,
                            "status": "SKIPPED_ALREADY_PROCESSED",
                            "raw_rows": meta.get("raw_rows", 0),
                            "valid_rows": meta.get("valid_rows", 0),
                            "invalid_rows": meta.get("invalid_rows", 0),
                            "elapsed_seconds": 0.0,
                            "mode": args.mode
                        })
                        continue
                    else:
                        print(f"[!] Rules hash mismatch for {taxi_type} {batch_month}. Re-processing...")
                        batch_status = "PROCESSED_RULE_CHANGED"
                except Exception:
                    print(f"[!] Corrupted _SUCCESS.json for {taxi_type} {batch_month}. Re-processing...")
                    batch_status = "PROCESSED_RULE_CHANGED"

            # Process batch
            t_batch_start = time.time()
            print(f"[+] PROCESSING: {taxi_type} {batch_month}...")

            try:
                # Ingestion
                if args.skip_download:
                    raw_filename = f"{taxi_type}_tripdata_{batch_month}.parquet"
                    raw_path = DATA_DIR / "raw" / raw_filename
                    if not raw_path.exists():
                        raise FileNotFoundError(f"Raw file not found: {raw_path}")
                else:
                    raw_path = download_raw_batch(taxi_type, batch_month, force=(args.mode == "force"))

                # Metadata check
                meta = inspect_raw_metadata(raw_path)
                print(f"    Raw size: {meta['size_mb']} MB | Rows: {meta['num_rows']:,}")

                # Schema Contract
                is_valid_schema, missing_cols = validate_schema_contract(raw_path, taxi_type)
                if not is_valid_schema:
                    raise ValueError(f"Schema Contract Violation for {taxi_type} {batch_month}: missing {missing_cols}")

                # Data Quality Audit
                dq_stat = audit_batch_data_quality(raw_path, taxi_type, batch_month)
                dq_records.append(dq_stat)

                # Transform & Atomic Load
                success_meta = transform_batch(raw_path, taxi_type, batch_month)
                t_batch_end = time.time()
                elapsed_batch = round(t_batch_end - t_batch_start, 2)

                print(f"    -> Done in {elapsed_batch}s. Valid: {success_meta['valid_rows']:,} | Invalid: {success_meta['invalid_rows']:,}")
                processed_count += 1

                run_records.append({
                    "run_id": run_id,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "taxi_type": taxi_type,
                    "batch_month": batch_month,
                    "status": batch_status,
                    "raw_rows": success_meta["raw_rows"],
                    "valid_rows": success_meta["valid_rows"],
                    "invalid_rows": success_meta["invalid_rows"],
                    "elapsed_seconds": elapsed_batch,
                    "mode": args.mode
                })
            except Exception as e:
                t_batch_end = time.time()
                elapsed_batch = round(t_batch_end - t_batch_start, 2)
                print(f"[!] BATCH FAILED: {taxi_type} {batch_month} - {e}")
                run_records.append({
                    "run_id": run_id,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "taxi_type": taxi_type,
                    "batch_month": batch_month,
                    "status": "FAILED",
                    "raw_rows": 0,
                    "valid_rows": 0,
                    "invalid_rows": 0,
                    "elapsed_seconds": elapsed_batch,
                    "mode": args.mode
                })
                # Persist any accumulated records before re-raising
                if dq_records:
                    update_dq_report(dq_records)
                if run_records:
                    append_pipeline_run_log(run_records)
                raise

    # Save DQ and run logs
    if dq_records:
        update_dq_report(dq_records)
    if run_records:
        append_pipeline_run_log(run_records)

    print("\n" + "=" * 80)
    print("RUNNING CURATION & ANALYSIS LAYER")
    print("=" * 80)

    # 3. Curation layer
    curate_res = generate_curated_outputs()
    print(f"[✓] Curated summary created ({curate_res['summary_rows']} rows)")
    print(f"[✓] Stratified sample exported ({curate_res['sample_rows']} rows)")

    # 4. Final Answer
    ans_res = answer_business_question()
    print(f"[✓] Final Answer Generated: WINNER = {ans_res['winner'].upper()}")
    print(f"    Trips: {ans_res['winner_trips']:,} | Market Share: {ans_res['winner_share']}%")

    # 5. Regression Check
    reg_res = verify_h1_regression()
    if reg_res["status"] == "PASSED":
        print(f"[✓] H1 REGRESSION CHECK PASSED: Total Valid = {reg_res['actual_total']:,} (Diff = 0)")
    else:
        print(f"[!] H1 REGRESSION CHECK {reg_res['status']}: Actual {reg_res.get('actual_total')} != Expected {reg_res.get('expected_total')}")

    # 6. Visualizations
    chart_path = generate_visualizations()
    print(f"[✓] Visualization chart saved to: {chart_path}")
    diagram_path = generate_pipeline_diagram()
    print(f"[✓] Pipeline diagram saved to: {diagram_path}")

    # 7. Sensitivity Analysis
    if not args.skip_sensitivity:
        sens_path = run_sensitivity_analysis()
        print(f"[✓] Sensitivity analysis saved to: {sens_path}")

    t_total = round(time.time() - t_start, 2)
    print("\n" + "=" * 80)
    print(f"PIPELINE RUN COMPLETED: {run_id}")
    print(f"Batches Processed: {processed_count} | Batches Skipped: {skipped_count} | Total Wall Time: {t_total}s")
    print("=" * 80)

    # Write Run Manifest Latest
    manifest = {
        "run_id": run_id,
        "pipeline_version": PIPELINE_VERSION,
        "mode": args.mode,
        "started_at_utc": start_utc.isoformat(),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_wall_time_seconds": t_total,
        "batches_processed": processed_count,
        "batches_skipped": skipped_count,
        "h1_regression_check": reg_res,
        "final_answer": {
            "winner": ans_res["winner"],
            "winner_trips": ans_res["winner_trips"],
            "winner_market_share_percent": ans_res["winner_share"]
        }
    }
    with open(LOGS_DIR / "run_manifest_latest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


if __name__ == "__main__":
    main()
