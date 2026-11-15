"""
src/analyze.py - Answer generation, regression verification, sensitivity, and visualization.
CS341 Big Data Engineering - Submission 2
"""
import glob
from pathlib import Path
import duckdb
import matplotlib.patches as patches
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


def generate_pipeline_diagram() -> Path:
    """
    Generate a clean Data Flow Diagram image (outputs/pipeline_data_flow.png)
    illustrating all 5 pipeline stages, storage zones, and output files.
    """
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_png = OUTPUTS_DIR / "pipeline_data_flow.png"

    plt.rcParams["font.sans-serif"] = ["Leelawadee UI", "Tahoma", "Sarabun", "sans-serif"]
    plt.rcParams["axes.unicode_minus"] = False

    fig, ax = plt.subplots(figsize=(15, 11.2), dpi=300)
    fig.patch.set_facecolor("#ffffff")
    ax.set_facecolor("#ffffff")
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    def draw_box(x, y, w, h, title, body, fc, ec, title_color="#0f172a", body_color="#334155"):
        box = patches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.35,rounding_size=1.2",
            facecolor=fc, edgecolor=ec, linewidth=1.8
        )
        ax.add_patch(box)
        ax.text(
            x + w / 2, y + h - 2.3, title,
            ha="center", va="top", fontsize=11.5, fontweight="bold", color=title_color
        )
        ax.text(
            x + w / 2, y + h - 5.3, body,
            ha="center", va="top", fontsize=9.5, color=body_color, linespacing=1.45
        )

    def draw_arrow(x1, y1, x2, y2, label=None, label_offset=(0, 0), color="#475569"):
        ax.annotate(
            "", xy=(x2, y2), xytext=(x1, y1),
            arrowprops=dict(arrowstyle="-|>,head_width=0.6,head_length=0.8", color=color, lw=1.8)
        )
        if label:
            mx = (x1 + x2) / 2 + label_offset[0]
            my = (y1 + y2) / 2 + label_offset[1]
            ax.text(
                mx, my, label, ha="center", va="center", fontsize=8.8, fontweight="bold",
                color=color, bbox=dict(boxstyle="round,pad=0.25", facecolor="#ffffff", edgecolor="none", alpha=0.9)
            )

    # Header
    ax.text(
        50, 96.5,
        "ผังการทำงานและการไหลของข้อมูล (NYC Taxi Reproducible Data Pipeline)",
        ha="center", va="center", fontsize=15.5, fontweight="bold", color="#0f172a"
    )
    ax.text(
        50, 93.2,
        "ประมวลผลข้อมูล 4 ประเภทแท็กซี่ (ม.ค.–มิ.ย. 2024 + ก.ค. 2024) ด้วย DuckDB Out-of-Core และ Atomic Writes",
        ha="center", va="center", fontsize=10.5, color="#475569"
    )

    # Step 1: Source
    draw_box(
        18, 80.5, 64, 9.2,
        "ขั้นที่ 1: แหล่งข้อมูลต้นทาง (NYC TLC CloudFront)",
        "ไฟล์ Parquet 4 ประเภท (yellow, green, fhv, fhvhv) ช่วงเดือน 2024-01 ถึง 2024-06 (+ 2024-07 สำหรับทดสอบอัปเดต)",
        fc="#f8fafc", ec="#64748b"
    )

    draw_arrow(50, 80.1, 50, 76.2)

    # Step 2: Ingest
    draw_box(
        14, 64.5, 72, 11.2,
        "ขั้นที่ 2: ดึงข้อมูลและตรวจสอบไฟล์ดิบ (src/ingest.py  ->  data/raw/)",
        "สตรีมดาวน์โหลดลงไฟล์ .tmp แล้วเปลี่ยนชื่อแบบ Atomic (os.replace) ป้องกันไฟล์เสียค้างกลางคัน\nอ่านจำนวนแถวและรายชื่อคอลัมน์จาก Parquet Footer ด้วย PyArrow โดยไม่โหลดข้อมูลเข้า RAM",
        fc="#eff6ff", ec="#3b82f6", title_color="#1e3a8a"
    )

    draw_arrow(
        50, 64.1, 50, 59.3,
        label="เช็ค Idempotency (run_pipeline.py): _SUCCESS.json + rules_hash ตรงกันให้ข้ามทันที",
        label_offset=(0, 0), color="#1d4ed8"
    )

    # Step 3: Validate & Transform
    draw_box(
        14, 47.0, 72, 11.8,
        "ขั้นที่ 3: ตรวจสอบ Schema และคัดกรองข้อมูล (src/validate.py & src/transform.py)",
        "เช็คคอลัมน์บังคับเทียบ config.SCHEMA_MAP (Fail-Fast) + ตรวจกฎเวลา 4 ข้อด้วย DuckDB Streaming\nตรวจสอบสมการสมดุล raw_rows == valid_rows + invalid_rows ก่อนสลับชื่อไฟล์และสร้าง _SUCCESS.json",
        fc="#fefce8", ec="#eab308", title_color="#713f12"
    )

    # Branching arrows from Step 3
    draw_arrow(34, 46.6, 25.0, 42.1, label="ผ่านเกณฑ์เวลา 4 ข้อ", label_offset=(-2.5, 0.3), color="#15803d")
    draw_arrow(66, 46.6, 75.0, 42.1, label="ไม่ผ่านเกณฑ์ (แยกตาม Priority)", label_offset=(2.5, 0.3), color="#b91c1c")

    # Box 3A: Trusted Zone
    draw_box(
        3, 27.5, 43.5, 14.0,
        "โซนข้อมูลสะอาด: data/trusted/ (7 คอลัมน์มาตรฐาน)",
        "• จัดเก็บแยกตาม taxi_type และ batch_month (ZSTD Parquet)\n• คงค่า pu/do_location_id เป็น Nullable เพื่อรักษาทริปของ FHV\n• เก็บ _SUCCESS.json คู่กับไฟล์ + ลดขนาด H1 เหลือ 1,357.44 MiB (-57.46%)",
        fc="#f0fdf4", ec="#22c55e", title_color="#14532d"
    )

    # Box 3B: Quarantine Zone
    draw_box(
        53.5, 27.5, 43.5, 14.0,
        "โซนข้อมูลมีปัญหา: data/quarantine/",
        "• เก็บแถวที่ตกเกณฑ์ลง rejected.parquet / .csv (H1 รวม 345 แถว)\n• ระบุ reject_reason ค่าเดียวตามลำดับสำคัญ (เช่น dropoff_before_pickup)\n• นับสถิติละเมิดกฎรายข้อแบบซ้อนทับเก็บไว้ใน logs/dq_report_by_batch.csv",
        fc="#fef2f2", ec="#ef4444", title_color="#7f1d1d"
    )

    draw_arrow(24.75, 27.1, 24.75, 22.8, color="#0369a1")
    draw_arrow(75.25, 27.1, 75.25, 22.8, label="สถิติ DQ & Log", color="#475569")

    # Box 4: Curate
    draw_box(
        3, 8.0, 43.5, 14.2,
        "ขั้นที่ 4: สร้างชุดข้อมูลพร้อมใช้ (src/curate.py)",
        "• data/curated/curated_monthly_summary.parquet / .csv\n  (ตารางสรุปรายเดือน 25 แถว ครอบคลุม 4 ประเภท + ก.ค. ของ Yellow)\n• data/sample/curated_trips_sample.parquet / .csv\n  (ตัวอย่างข้อมูลพร้อมใช้ 1,000 แถว — สุ่มแบ่งชั้น batch ละ 40 แถว)",
        fc="#f0f9ff", ec="#0284c7", title_color="#0c4a6e"
    )

    draw_arrow(47.0, 15.1, 53.0, 15.1, color="#6d28d9")

    # Box 5: Analyze
    draw_box(
        53.5, 8.0, 43.5, 14.2,
        "ขั้นที่ 5: วิเคราะห์คำตอบและบันทึกหลักฐาน (src/analyze.py)",
        "• กรองเฉพาะ batch_month ช่วง H1 (2024-01..2024-06) ตอบคำถามหลัก\n• outputs/final_answer_by_taxi_type.csv (ผู้ชนะ: FHVHV 120.86M / 80.76%)\n• เช็ค H1 Regression ตรงกับ S1 (149,655,451 แถว, Diff = 0)\n• สร้างกราฟผลสรุป, ตาราง Sensitivity และบันทึกประวัติใน logs/",
        fc="#faf5ff", ec="#9333ea", title_color="#4c1d95"
    )

    # Footer: Cloud Serving Script
    footer_box = patches.FancyBboxPatch(
        (3, 1.5), 94, 4.3,
        boxstyle="round,pad=0.25,rounding_size=0.8",
        facecolor="#f1f5f9", edgecolor="#94a3b8", linewidth=1.2, linestyle="--"
    )
    ax.add_patch(footer_box)
    ax.text(
        50, 3.65,
        "การต่อยอดขึ้น Cloud (AWS S3 + Amazon Athena): ใช้ scripts/deploy_athena.py และ sql/athena_setup.sql สำหรับนำไฟล์ data/curated/ ขึ้นสร้าง Serving View บน Cloud",
        ha="center", va="center", fontsize=9.2, color="#334155", fontweight="bold"
    )

    plt.tight_layout()
    plt.savefig(output_png, dpi=300, bbox_inches="tight", pad_inches=0.2)
    plt.close()

    return output_png
