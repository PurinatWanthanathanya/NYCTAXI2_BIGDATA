"""
src/analyze.py - Answer generation, regression verification, sensitivity, and visualization.
CS341 Big Data Engineering - Submission 2
"""
import glob
from pathlib import Path
import duckdb
import matplotlib.pyplot as plt
import pandas as pd

from config import (
    BASELINE_H1_EXPECTED,
    CURATED_DIR,
    OUTPUTS_DIR,
    TRUSTED_DIR
)


def answer_business_question() -> dict:
    """
    Query curated monthly summary to answer:
    'Which taxi type was the most used in the first half of 2024?'
    Filters strictly on batch_month BETWEEN '2024-01' AND '2024-06'.
    """
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = CURATED_DIR / "curated_monthly_summary.parquet"
    if not summary_path.exists():
        raise FileNotFoundError(f"Curated summary not found at {summary_path}")

    con = duckdb.connect()
    ranking_sql = f"""
    WITH h1_data AS (
        SELECT
            taxi_type,
            SUM(total_trips) AS total_trips
        FROM read_parquet('{str(summary_path).replace("\\", "/")}')
        WHERE batch_month BETWEEN '2024-01' AND '2024-06'
        GROUP BY taxi_type
    ),
    total_calc AS (
        SELECT SUM(total_trips) AS grand_total FROM h1_data
    )
    SELECT
        h.taxi_type,
        h.total_trips,
        ROUND(h.total_trips * 100.0 / t.grand_total, 2) AS market_share_percent,
        RANK() OVER (ORDER BY h.total_trips DESC) AS rank
    FROM h1_data h, total_calc t
    ORDER BY rank ASC;
    """
    df_ranking = con.execute(ranking_sql).df()
    con.close()

    output_csv = OUTPUTS_DIR / "final_answer_by_taxi_type.csv"
    df_ranking.to_csv(output_csv, index=False)

    winner = df_ranking.iloc[0]["taxi_type"]
    winner_trips = int(df_ranking.iloc[0]["total_trips"])
    winner_share = float(df_ranking.iloc[0]["market_share_percent"])

    return {
        "ranking_df": df_ranking,
        "output_csv": str(output_csv),
        "winner": winner,
        "winner_trips": winner_trips,
        "winner_share": winner_share
    }


def verify_h1_regression() -> dict:
    """
    Regression check against Submission 1 baseline numbers (149,655,451 valid trips).
    """
    summary_path = CURATED_DIR / "curated_monthly_summary.parquet"
    if not summary_path.exists():
        return {"status": "SKIPPED", "reason": "Curated summary not found"}

    con = duckdb.connect()
    check_sql = f"""
    SELECT
        taxi_type,
        SUM(total_trips) AS total_valid
    FROM read_parquet('{str(summary_path).replace("\\", "/")}')
    WHERE batch_month BETWEEN '2024-01' AND '2024-06'
    GROUP BY taxi_type;
    """
    actual_records = dict(con.execute(check_sql).fetchall())
    con.close()

    actual_total = sum(actual_records.values())
    expected_total = BASELINE_H1_EXPECTED["total_valid"]

    type_checks = {}
    for t in ["yellow", "green", "fhv", "fhvhv"]:
        act = actual_records.get(t, 0)
        exp = BASELINE_H1_EXPECTED[f"{t}_valid"]
        type_checks[t] = {
            "actual": act,
            "expected": exp,
            "match": (act == exp)
        }

    total_match = (actual_total == expected_total)

    return {
        "status": "PASSED" if total_match else "FAILED",
        "actual_total": actual_total,
        "expected_total": expected_total,
        "diff": actual_total - expected_total,
        "type_checks": type_checks
    }


def run_sensitivity_analysis() -> Path:
    """
    Sensitivity Analysis: evaluate impact of optional cleaning rules on winner.
    Checks:
      1. Baseline clean (current pipeline)
      2. Excluding 0-second duration trips
      3. Excluding >24-hour duration trips
      4. Excluding location-based duplicates (excluding NULL locations)
    """
    trusted_pattern = str(TRUSTED_DIR / "taxi_type=*" / "batch_month=*" / "data.parquet").replace("\\", "/")
    output_csv = OUTPUTS_DIR / "sensitivity_analysis.csv"

    con = duckdb.connect()

    # Base counts for H1
    base_sql = f"""
    SELECT
        taxi_type,
        COUNT(*) AS base_trips,
        COUNT(CASE WHEN trip_duration_seconds = 0 THEN 1 END) AS zero_sec_trips,
        COUNT(CASE WHEN trip_duration_seconds > 86400 THEN 1 END) AS gt_24h_trips
    FROM read_parquet('{trusted_pattern}')
    WHERE batch_month BETWEEN '2024-01' AND '2024-06'
    GROUP BY taxi_type;
    """
    df_base = con.execute(base_sql).df().set_index("taxi_type")

    # Non-null duplicate check
    dup_sql = f"""
    SELECT
        taxi_type,
        COUNT(*) - COUNT(DISTINCT (pickup_ts, dropoff_ts, pu_location_id, do_location_id)) AS dup_trips
    FROM read_parquet('{trusted_pattern}')
    WHERE batch_month BETWEEN '2024-01' AND '2024-06'
      AND pu_location_id IS NOT NULL AND do_location_id IS NOT NULL
    GROUP BY taxi_type;
    """
    df_dup = con.execute(dup_sql).df().set_index("taxi_type")
    con.close()

    df_combined = df_base.join(df_dup, how="left").fillna(0)

    rows = []
    scenarios = [
        ("1. Baseline (Official Rules)", lambda r: r["base_trips"]),
        ("2. Exclude 0-sec duration", lambda r: r["base_trips"] - r["zero_sec_trips"]),
        ("3. Exclude >24h duration", lambda r: r["base_trips"] - r["gt_24h_trips"]),
        ("4. Exclude duplicate non-null trips", lambda r: r["base_trips"] - r["dup_trips"]),
    ]

    for name, fn in scenarios:
        counts = {t: int(fn(df_combined.loc[t])) for t in df_combined.index}
        sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        winner, win_count = sorted_counts[0]
        if len(sorted_counts) > 1:
            runner_up, run_count = sorted_counts[1]
            margin = win_count - run_count
        else:
            runner_up, run_count = "N/A", 0
            margin = win_count

        rows.append({
            "scenario": name,
            "fhvhv_trips": counts.get("fhvhv", 0),
            "yellow_trips": counts.get("yellow", 0),
            "fhv_trips": counts.get("fhv", 0),
            "green_trips": counts.get("green", 0),
            "winner": winner,
            "margin_over_runner_up": margin,
            "winner_changed": (winner != "fhvhv" if "fhvhv" in counts else False)
        })

    df_result = pd.DataFrame(rows)
    df_result.to_csv(output_csv, index=False)
    return output_csv


def generate_visualizations() -> Path:
    """
    Generate publication-ready visualization of H1 2024 NYC Taxi Usage.
    Subplot 1: Total Trips by Taxi Type (Bar chart with market share).
    Subplot 2: Monthly Trips Trend across H1 2024.
    """
    summary_path = CURATED_DIR / "curated_monthly_summary.parquet"
    output_png = OUTPUTS_DIR / "taxi_usage_h1_2024.png"

    con = duckdb.connect()
    # H1 totals
    h1_df = con.execute(f"""
        SELECT
            taxi_type,
            SUM(total_trips) AS total_trips
        FROM read_parquet('{str(summary_path).replace("\\", "/")}')
        WHERE batch_month BETWEEN '2024-01' AND '2024-06'
        GROUP BY taxi_type
        ORDER BY total_trips DESC;
    """).df()

    # Monthly breakdown
    monthly_df = con.execute(f"""
        SELECT
            batch_month,
            taxi_type,
            total_trips
        FROM read_parquet('{str(summary_path).replace("\\", "/")}')
        WHERE batch_month BETWEEN '2024-01' AND '2024-06'
        ORDER BY batch_month, taxi_type;
    """).df()
    con.close()

    colors = {
        "fhvhv": "#1f77b4",  # Blue
        "yellow": "#ff7f0e", # Amber/Yellow
        "fhv": "#2ca02c",    # Greenish
        "green": "#8c564b"   # Brown/Green
    }

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # 1. Bar Chart
    total_sum = h1_df["total_trips"].sum()
    bars = ax1.bar(
        h1_df["taxi_type"].str.upper(),
        h1_df["total_trips"] / 1e6,
        color=[colors.get(t, "#333333") for t in h1_df["taxi_type"]]
    )
    ax1.set_title("NYC Taxi Total Trips (H1 2024)", fontsize=14, fontweight="bold")
    ax1.set_ylabel("Trips (Millions)", fontsize=12)
    ax1.set_xlabel("Taxi Type", fontsize=12)
    ax1.grid(axis="y", linestyle="--", alpha=0.7)

    for bar in bars:
        height = bar.get_height()
        share = (height * 1e6 / total_sum) * 100
        ax1.annotate(
            f"{height:.2f}M\n({share:.1f}%)",
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center", va="bottom", fontsize=10, fontweight="bold"
        )

    # 2. Monthly Trend Line Chart
    pivoted = monthly_df.pivot(index="batch_month", columns="taxi_type", values="total_trips") / 1e6
    for t in ["fhvhv", "yellow", "fhv", "green"]:
        if t in pivoted.columns:
            ax2.plot(
                pivoted.index,
                pivoted[t],
                marker="o",
                linewidth=2.5,
                label=t.upper(),
                color=colors.get(t, "#333333")
            )

    ax2.set_title("Monthly Trip Trends (2024-01 to 2024-06)", fontsize=14, fontweight="bold")
    ax2.set_ylabel("Trips (Millions)", fontsize=12)
    ax2.set_xlabel("Month", fontsize=12)
    ax2.grid(True, linestyle="--", alpha=0.7)
    ax2.legend(title="Taxi Type", fontsize=10)

    plt.tight_layout()
    plt.savefig(output_png, dpi=300)
    plt.close()

    return output_png
