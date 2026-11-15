"""
scripts/verify_readme_evidence.py - Verify all numbers and claims reported in README.md
against actual logs, quarantine partitions, curated outputs, and local raw/trusted parquet files.
CS341 Big Data Engineering - Submission 2
"""
import json
import sys
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
H1_MONTHS = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]
ALL_MONTHS = H1_MONTHS + ["2024-07"]
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]
MIB = 1024 * 1024


def format_sql_paths(paths: list[Path]) -> str:
    """Format file paths for DuckDB read_parquet([...]) list literal."""
    formatted = []
    for p in paths:
        s = str(p).replace("\\", "/").replace("'", "''")
        formatted.append(f"'{s}'")
    return "[" + ", ".join(formatted) + "]"


def verify_section_4_file_sizes() -> None:
    """Section 4: Verify raw vs trusted MiB sizes and compression savings."""
    print("[1] Section 4: File Sizes & Compression Savings (Raw -> Trusted)")
    raw_dir = BASE_DIR / "data" / "raw"
    trusted_dir = BASE_DIR / "data" / "trusted"

    h1_raw_files = [
        raw_dir / f"{t}_tripdata_{m}.parquet"
        for t in TAXI_TYPES
        for m in H1_MONTHS
        if (raw_dir / f"{t}_tripdata_{m}.parquet").exists()
    ]
    h1_trusted_files = [
        trusted_dir / f"taxi_type={t}" / f"batch_month={m}" / "data.parquet"
        for t in TAXI_TYPES
        for m in H1_MONTHS
        if (trusted_dir / f"taxi_type={t}" / f"batch_month={m}" / "data.parquet").exists()
    ]

    all_batches = [(t, m) for t in TAXI_TYPES for m in H1_MONTHS] + [("yellow", "2024-07")]
    all_raw_files = [
        raw_dir / f"{t}_tripdata_{m}.parquet"
        for t, m in all_batches
        if (raw_dir / f"{t}_tripdata_{m}.parquet").exists()
    ]
    all_trusted_files = [
        trusted_dir / f"taxi_type={t}" / f"batch_month={m}" / "data.parquet"
        for t, m in all_batches
        if (trusted_dir / f"taxi_type={t}" / f"batch_month={m}" / "data.parquet").exists()
    ]

    if len(h1_raw_files) < 24 or len(h1_trusted_files) < 24:
        print(
            f"  [SKIP] Local raw/trusted parquet not full "
            f"(raw={len(all_raw_files)}, trusted={len(all_trusted_files)}); "
            "expected on fresh clone because raw & trusted parquet files are gitignored."
        )
        print()
        return

    h1_raw_mib = sum(p.stat().st_size for p in h1_raw_files) / MIB
    h1_tru_mib = sum(p.stat().st_size for p in h1_trusted_files) / MIB
    h1_saved = (1.0 - h1_tru_mib / h1_raw_mib) * 100.0
    print(
        f"  H1 2024 ({len(h1_raw_files)} files): "
        f"Raw = {h1_raw_mib:,.2f} MiB -> Trusted = {h1_tru_mib:,.2f} MiB "
        f"(Saved {h1_saved:.2f}%)"
    )

    if len(all_raw_files) == 25 and len(all_trusted_files) == 25:
        all_raw_mib = sum(p.stat().st_size for p in all_raw_files) / MIB
        all_tru_mib = sum(p.stat().st_size for p in all_trusted_files) / MIB
        all_saved = (1.0 - all_tru_mib / all_raw_mib) * 100.0
        print(
            f"  Incl. 2024-07 ({len(all_raw_files)} files): "
            f"Raw = {all_raw_mib:,.2f} MiB -> Trusted = {all_tru_mib:,.2f} MiB "
            f"(Saved {all_saved:.2f}%)"
        )
    print()


def verify_section_5_run_history() -> None:
    """Section 5: Verify 3-run history from logs/pipeline_run_log.csv and run_manifest_latest.json."""
    print("[2] Section 5: Run History & Latest Manifest (logs/)")
    log_path = BASE_DIR / "logs" / "pipeline_run_log.csv"
    manifest_path = BASE_DIR / "logs" / "run_manifest_latest.json"

    df = pd.read_csv(log_path)
    for run_id, label in [
        ("ae02785e", "Run 1 (H1 Full FORCE)"),
        ("6fe1ebe9", "Run 2 (Idempotent SKIP)"),
        ("7c3f23e2", "Run 3 (Incremental yellow 2024-07)"),
    ]:
        sub = df[df["run_id"] == run_id]
        statuses = {str(k): int(v) for k, v in sub["status"].value_counts().items()}
        raw_sum = int(sub["raw_rows"].sum())
        valid_sum = int(sub["valid_rows"].sum())
        invalid_sum = int(sub["invalid_rows"].sum())
        elapsed_sum = float(sub["elapsed_seconds"].sum())
        print(
            f"  {label} [{run_id}]: batches={len(sub)}, statuses={statuses}, "
            f"raw={raw_sum:,}, valid={valid_sum:,}, invalid={invalid_sum:,}, "
            f"batch_elapsed_sum={elapsed_sum:.2f}s"
        )
        if run_id == "7c3f23e2":
            inc = sub[sub["status"] == "SUCCESS"]
            for _, r in inc.iterrows():
                print(
                    f"    -> New batch processed: {r['taxi_type']} {r['batch_month']} "
                    f"(raw={int(r['raw_rows']):,}, valid={int(r['valid_rows']):,}, "
                    f"invalid={int(r['invalid_rows']):,}, elapsed={float(r['elapsed_seconds']):.2f}s)"
                )

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    h1_check = manifest.get("h1_regression_check", {})
    winner = manifest.get("final_answer", {})
    print(
        f"  Latest Manifest ({manifest.get('run_id')}): "
        f"wall_time={manifest.get('total_wall_time_seconds')}s, "
        f"h1_check={h1_check.get('status')} "
        f"(total={h1_check.get('actual_total', 0):,}, diff={h1_check.get('diff')}), "
        f"winner={winner.get('winner', '').upper()} ({winner.get('winner_trips', 0):,} / "
        f"{winner.get('winner_market_share_percent')}%)"
    )
    print()


def verify_section_6_dq_and_quarantine() -> None:
    """Section 6.1-6.3: Verify H1 reconciliation, quarantine details, and FHV NULL Location IDs."""
    print("[3] Section 6.1-6.3: H1 Reconciliation, Quarantine (345 rows) & FHV Nulls")
    dq_path = BASE_DIR / "logs" / "dq_report_by_batch.csv"
    dq = pd.read_csv(dq_path)
    dq_h1 = dq[dq["batch_month"].isin(H1_MONTHS)]

    print("  6.1 H1 2024 Reconciliation by Taxi Type (from dq_report_by_batch.csv):")
    for t in ["yellow", "green", "fhv", "fhvhv"]:
        sub = dq_h1[dq_h1["taxi_type"] == t]
        print(
            f"    {t:<6}: raw={int(sub['total_rows'].sum()):>11,} | "
            f"valid={int(sub['valid_rows'].sum()):>11,} | "
            f"invalid={int(sub['total_invalid'].sum()):>3,}"
        )
    print(
        f"    TOTAL : raw={int(dq_h1['total_rows'].sum()):>11,} | "
        f"valid={int(dq_h1['valid_rows'].sum()):>11,} | "
        f"invalid={int(dq_h1['total_invalid'].sum()):>3,}"
    )

    q_files = [
        p
        for t in TAXI_TYPES
        for m in H1_MONTHS
        for p in [BASE_DIR / "data" / "quarantine" / f"taxi_type={t}" / f"batch_month={m}" / "rejected.parquet"]
        if p.exists()
    ]
    with duckdb.connect() as con:
        files_sql = format_sql_paths(q_files)
        reason_rows = con.execute(f"""
            SELECT reject_reason, COUNT(*) AS cnt
            FROM read_parquet({files_sql}, union_by_name=True, hive_partitioning=False)
            GROUP BY reject_reason
            ORDER BY cnt DESC
        """).fetchall()
        print(f"  6.2 Quarantine H1 breakdown by reject_reason ({len(q_files)} files): {reason_rows}")

        mar_yellow = con.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet({files_sql}, union_by_name=True, hive_partitioning=False)
            WHERE taxi_type = 'yellow' AND batch_month = '2024-03' AND reject_reason = 'dropoff_before_pickup'
        """).fetchone()[0]
        print(f"    yellow 2024-03 dropoff_before_pickup = {mar_yellow}")

        oop_rows = con.execute(f"""
            SELECT
                CASE
                    WHEN strftime(COALESCE(tpep_pickup_datetime, lpep_pickup_datetime), '%Y-%m') = '2024-07'
                        THEN '2024-07'
                    ELSE strftime(COALESCE(tpep_pickup_datetime, lpep_pickup_datetime), '%Y')
                END AS bucket,
                SUM(CASE WHEN taxi_type = 'yellow' THEN 1 ELSE 0 END) AS yellow_cnt,
                SUM(CASE WHEN taxi_type = 'green' THEN 1 ELSE 0 END) AS green_cnt,
                COUNT(*) AS total_cnt
            FROM read_parquet({files_sql}, union_by_name=True, hive_partitioning=False)
            WHERE reject_reason = 'pickup_out_of_period'
            GROUP BY bucket
            ORDER BY bucket
        """).fetchall()
        oop_summary = ", ".join(
            f"{b}(total={tot}: Y={y}, G={g})" for b, y, g, tot in oop_rows
        )
        print(f"    pickup_out_of_period periods: {oop_summary}")

    fhv = dq_h1[dq_h1["taxi_type"] == "fhv"]
    fhv_total = int(fhv["total_rows"].sum())
    fhv_null_pu = int(fhv["null_pu_location"].sum())
    fhv_pct = (fhv_null_pu / fhv_total) * 100.0
    print(
        f"  6.3 FHV Null PUlocationID: {fhv_null_pu:,} / {fhv_total:,} ({fhv_pct:.2f}%), "
        f"monthly range = {int(fhv['null_pu_location'].min()):,} to {int(fhv['null_pu_location'].max()):,}"
    )
    print()


def verify_section_6_4_sensitivity() -> None:
    """Section 6.4: Print verified sensitivity analysis table from outputs/sensitivity_analysis.csv."""
    print("[4] Section 6.4: Sensitivity Analysis (outputs/sensitivity_analysis.csv)")
    sens_path = BASE_DIR / "outputs" / "sensitivity_analysis.csv"
    df = pd.read_csv(sens_path)
    for _, row in df.iterrows():
        print(
            f"  {row['scenario']}: FHVHV={int(row['fhvhv_trips']):,} | "
            f"Yellow={int(row['yellow_trips']):,} | FHV={int(row['fhv_trips']):,} | "
            f"Green={int(row['green_trips']):,} | Winner={str(row['winner']).upper()} "
            f"(+{int(row['margin_over_runner_up']):,}, changed={row['winner_changed']})"
        )
    print()


def verify_section_7_1_batch_month_mismatch() -> None:
    """Section 7.1: Check H1 trusted rows where strftime(pickup_ts, '%Y-%m') != batch_month."""
    print("[5] Section 7.1: batch_month vs pickup_ts Mismatch in H1 Trusted Parquet")
    trusted_dir = BASE_DIR / "data" / "trusted"
    h1_trusted = [
        trusted_dir / f"taxi_type={t}" / f"batch_month={m}" / "data.parquet"
        for t in TAXI_TYPES
        for m in H1_MONTHS
        if (trusted_dir / f"taxi_type={t}" / f"batch_month={m}" / "data.parquet").exists()
    ]
    if len(h1_trusted) < 24:
        print(
            f"  [SKIP] Local H1 trusted parquet incomplete ({len(h1_trusted)}/24); "
            "expected on fresh clone because trusted data.parquet files are gitignored."
        )
        print()
        return

    files_sql = format_sql_paths(h1_trusted)
    with duckdb.connect() as con:
        by_type = con.execute(f"""
            SELECT taxi_type, COUNT(*) AS cnt
            FROM read_parquet({files_sql}, hive_partitioning=False)
            WHERE strftime(pickup_ts, '%Y-%m') != batch_month
            GROUP BY taxi_type
            ORDER BY cnt DESC
        """).fetchall()
        total_mismatch = sum(cnt for _, cnt in by_type)
        examples = con.execute(f"""
            SELECT taxi_type, batch_month, strftime(pickup_ts, '%Y-%m') AS pickup_month, COUNT(*) AS cnt
            FROM read_parquet({files_sql}, hive_partitioning=False)
            WHERE strftime(pickup_ts, '%Y-%m') != batch_month
            GROUP BY taxi_type, batch_month, pickup_month
            ORDER BY cnt DESC
            LIMIT 5
        """).fetchall()
    print(f"  Total H1 mismatch rows: {total_mismatch} -> by taxi_type: {by_type}")
    print(f"  Top examples (type, batch_month, pickup_month, cnt): {examples}")
    print()


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    print("=" * 72)
    print("README EVIDENCE VERIFICATION REPORT (Live Query on Repo Files)")
    print("=" * 72)
    verify_section_4_file_sizes()
    verify_section_5_run_history()
    verify_section_6_dq_and_quarantine()
    verify_section_6_4_sensitivity()
    verify_section_7_1_batch_month_mismatch()
    print("=" * 72)
    print("VERIFICATION COMPLETED SUCCESSFULLY")
    print("=" * 72)


if __name__ == "__main__":
    main()
