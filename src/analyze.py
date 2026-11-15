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
    Generate a detailed Data Flow Diagram image (outputs/pipeline_data_flow.png)
    illustrating idempotency skip checks, ingest, pre-transform schema validation
    PASS/FAIL gates, transform reconciliation (trusted vs quarantine), curation,
    and analysis/serving.
    """
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_png = OUTPUTS_DIR / "pipeline_data_flow.png"

    plt.rcParams["font.sans-serif"] = ["Leelawadee UI", "Tahoma", "Sarabun", "sans-serif"]
    plt.rcParams["axes.unicode_minus"] = False

    fig, ax = plt.subplots(figsize=(16, 13.5), dpi=300)
    fig.patch.set_facecolor("#ffffff")
    ax.set_facecolor("#ffffff")
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    def draw_box(
        x, y, w, h, title, body, fc, ec,
        title_color="#0f172a", body_color="#334155",
        title_fs=10.6, body_fs=8.6, linestyle="-"
    ):
        box = patches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.3,rounding_size=1.0",
            facecolor=fc, edgecolor=ec, linewidth=1.7, linestyle=linestyle
        )
        ax.add_patch(box)
        ax.text(
            x + w / 2, y + h - 1.25, title,
            ha="center", va="top", fontsize=title_fs, fontweight="bold", color=title_color
        )
        ax.text(
            x + w / 2, y + h - 3.65, body,
            ha="center", va="top", fontsize=body_fs, color=body_color, linespacing=1.38
        )

    def draw_arrow(
        x1, y1, x2, y2, label=None, label_pos=None,
        color="#475569", linestyle="-", label_fs=8.2
    ):
        ax.annotate(
            "", xy=(x2, y2), xytext=(x1, y1),
            arrowprops=dict(
                arrowstyle="-|>,head_width=0.55,head_length=0.75",
                color=color, lw=1.7, linestyle=linestyle
            )
        )
        if label:
            lx, ly = label_pos if label_pos is not None else ((x1 + x2) / 2, (y1 + y2) / 2)
            ax.text(
                lx, ly, label, ha="center", va="center", fontsize=label_fs, fontweight="bold",
                color=color, bbox=dict(boxstyle="round,pad=0.22", facecolor="#ffffff", edgecolor="#e2e8f0", lw=0.7, alpha=0.96)
            )

    def draw_polyline_arrow(points, label=None, label_pos=None, color="#475569", linestyle="--", label_fs=8.0):
        xs = [p[0] for p in points[:-1]]
        ys = [p[1] for p in points[:-1]]
        ax.plot(xs, ys, color=color, lw=1.7, linestyle=linestyle, solid_capstyle="round")
        draw_arrow(
            points[-2][0], points[-2][1], points[-1][0], points[-1][1],
            label=label, label_pos=label_pos, color=color, linestyle=linestyle, label_fs=label_fs
        )

    # Header
    ax.text(
        50, 97.4,
        "ผังการทำงานและการไหลของข้อมูล (NYC Taxi Reproducible Data Pipeline)",
        ha="center", va="center", fontsize=15.0, fontweight="bold", color="#0f172a"
    )
    ax.text(
        50, 94.7,
        "แสดงด่านตรวจสอบ Idempotency, ด่านตรวจ Schema ก่อน Transform, เส้นทาง PASS / FAIL และโซนข้อมูลทั้งหมด",
        ha="center", va="center", fontsize=10.0, color="#475569"
    )

    # Row 1: Source & Gate 1 (Idempotency)
    draw_box(
        3.0, 83.0, 32.0, 9.2,
        "แหล่งข้อมูล (NYC TLC CloudFront)",
        "• ไฟล์ Parquet 4 ประเภท (yellow, green, fhv, fhvhv)\n• ช่วง H1: 2024-01 ถึง 2024-06 (24 ไฟล์)\n• + ทดสอบอัปเดตเดือนใหม่: yellow 2024-07",
        fc="#f8fafc", ec="#64748b", title_color="#0f172a"
    )
    draw_arrow(35.4, 87.6, 40.6, 87.6, label="ตรวจราย Batch", label_pos=(38.0, 89.2), color="#475569", label_fs=7.6)

    draw_box(
        41.0, 83.0, 56.0, 9.2,
        "ด่านที่ 1: เช็ค Idempotency (run_pipeline.py & config.py)",
        "ตรวจ _SUCCESS.json + data.parquet + ค่า rules_hash (SHA-256 ของ Schema & กฎเวลา)\n• SKIP (--mode skip & hash ตรง): ข้าม Batch ตรงไปขั้น Curate (< 0.05s/ไฟล์)\n• RUN (ไฟล์ใหม่ / --mode force / rules_hash เปลี่ยน): ส่งเข้าขั้นที่ 1 Ingest",
        fc="#eef2ff", ec="#4f46e5", title_color="#312e81"
    )

    # Arrow from Gate 1 -> Step 1 (RUN)
    draw_arrow(
        49.0, 82.6, 49.0, 78.6,
        label="RUN: ไฟล์ใหม่ / force / กฎเปลี่ยน",
        label_pos=(49.0, 80.6), color="#1d4ed8", label_fs=8.0
    )

    # Row 2: Step 1 (Ingest -> data/raw)
    draw_box(
        3.0, 69.0, 58.0, 9.2,
        "ขั้นที่ 1: ดึงไฟล์ดิบลง Raw Zone (src/ingest.py  ->  data/raw/)",
        "• สตรีมดาวน์โหลดลง .parquet.tmp แล้วสลับชื่อด้วย os.replace() กันไฟล์ขาดกลางคัน\n• ใช้ PyArrow อ่านเฉพาะ Parquet Footer เอา num_rows & columns โดยไม่โหลดทั้งไฟล์เข้า RAM\n• เก็บไฟล์ดิบครบ 24 ไฟล์ H1 (3,191.23 MiB) โดยห้ามแก้ไขทับข้อมูลต้นฉบับ",
        fc="#eff6ff", ec="#3b82f6", title_color="#1e3a8a"
    )

    # Arrow from Step 1 -> Step 2 (Validate)
    draw_arrow(
        32.0, 68.6, 32.0, 63.4,
        label="ส่งไฟล์ดิบไปตรวจโครงสร้างก่อน Transform",
        label_pos=(32.0, 66.0), color="#b45309", label_fs=8.0
    )

    # Row 3: Step 2 (Validation Gate) & FAIL branch
    draw_box(
        3.0, 53.0, 58.0, 10.0,
        "ขั้นที่ 2: ด่านตรวจ Schema & นับ DQ ก่อน Transform (src/validate.py)",
        "1) ตรวจ Schema Contract: ต้องมีคอลัมน์ครบ 4 ตัวตาม config.SCHEMA_MAP\n   (จัดการชื่อที่ต่างกัน เช่น tpep_*, lpep_*, dropOff_datetime, PUlocationID)\n2) คิวรีนับสถิติ DQ (overlap_* vs prio_*) + ค่าว่าง PUlocationID บันทึก dq_report_by_batch.csv",
        fc="#fefce8", ec="#eab308", title_color="#713f12"
    )

    draw_arrow(
        61.4, 58.0, 65.6, 58.0,
        label="FAIL", label_pos=(63.5, 59.6), color="#dc2626", label_fs=8.2
    )

    draw_box(
        66.0, 53.0, 25.5, 10.0,
        "FAIL: หยุดแบบ Fail-Fast",
        "• คอลัมน์หายหรือไฟล์เสีย\n• หยุด Batch ทันทีก่อน Transform\n• บันทึก status = FAILED\n  ลง logs/pipeline_run_log.csv",
        fc="#fff1f2", ec="#e11d48", title_color="#881337"
    )

    # Arrow Step 2 -> Step 3 (PASS Schema)
    draw_arrow(
        32.0, 52.6, 32.0, 48.0,
        label="PASS: Schema ครบถ้วน",
        label_pos=(32.0, 50.3), color="#15803d", label_fs=8.2
    )

    # Row 4: Step 3 (Transform & Reconciliation)
    draw_box(
        3.0, 38.0, 58.0, 9.6,
        "ขั้นที่ 3: คัดกรองแถวและตรวจสมการสมดุล (src/transform.py)",
        "• ใช้ DuckDB Streaming กรองกฎเวลา 4 ข้อ: null_pickup, null_dropoff, dropoff < pickup, นอกช่วงเวลา\n• แปลงชื่อเป็น 7 คอลัมน์มาตรฐาน แล้วเขียนแยก .tmp พร้อมตรวจ assert raw == valid + invalid\n• เมื่อตัวเลขสมดุลตรงกันจึงทำ os.replace() เพื่อเปลี่ยนชื่อไฟล์จริงและออกใบรับรอง",
        fc="#fff7ed", ec="#f97316", title_color="#7c2d12"
    )

    # Branch arrows from Transform -> Trusted (PASS) and Quarantine (FAIL)
    draw_arrow(
        24.0, 37.6, 24.0, 33.4,
        label="PASS กฎเวลา 4 ข้อ (149,655,451 แถว)",
        label_pos=(24.0, 35.5), color="#15803d", label_fs=8.0
    )
    draw_arrow(
        52.0, 37.6, 68.0, 33.4,
        label="FAIL กฎเวลา (345 แถว H1)",
        label_pos=(60.0, 35.5), color="#dc2626", label_fs=8.0
    )

    # Row 5: Box 3A (Trusted) & Box 3B (Quarantine)
    draw_box(
        3.0, 22.5, 45.0, 10.5,
        "PASS -> โซนข้อมูลสะอาด: data/trusted/",
        "• เก็บ 7 คอลัมน์มาตรฐาน บีบอัด ZSTD แยกตาม taxi_type & batch_month\n• อนุญาตให้ pu/do_location_id เป็น NULL (รักษาข้อมูล FHV 78.33%)\n• ลดขนาด H1 เหลือ 1,357.44 MiB (-57.46% จากไฟล์ดิบ)\n• สร้างไฟล์ _SUCCESS.json เก็บ rules_hash ไว้เป็นขั้นตอนสุดท้าย",
        fc="#f0fdf4", ec="#22c55e", title_color="#14532d"
    )

    draw_box(
        51.5, 22.5, 40.0, 10.5,
        "FAIL -> โซนข้อมูลมีปัญหา: data/quarantine/",
        "• แยกเก็บใน rejected.parquet และ rejected.csv (เปิดตรวจได้)\n• ระบุ reject_reason ค่าเดียวตามลำดับความสำคัญ:\n  dropoff_before_pickup (300 แถว) | pickup_out_of_period (45 แถว)\n• ไม่ให้ข้อมูลเสียหลุดเข้าข้อมูลสะอาดและใช้ตรวจสอบย้อนหลังได้",
        fc="#fef2f2", ec="#ef4444", title_color="#7f1d1d"
    )

    # Trusted -> Curate arrow & Gate 1 SKIP Express Lane -> Curate
    draw_arrow(
        23.0, 22.1, 23.0, 17.9,
        label="อ่านไฟล์สะอาดทุกพาร์ทิชัน",
        label_pos=(23.0, 20.0), color="#0369a1", label_fs=8.0
    )

    draw_polyline_arrow(
        [(95.2, 82.6), (95.2, 19.9), (38.0, 19.9), (38.0, 17.9)],
        label="SKIP (hash ตรง)\nข้ามไปขั้น Curate",
        label_pos=(93.8, 44.5), color="#4f46e5", linestyle="--", label_fs=7.8
    )

    # Row 6: Curate & Analyze
    draw_box(
        3.0, 6.5, 45.0, 11.0,
        "ขั้นที่ 4: สร้างชุดข้อมูลพร้อมใช้ (src/curate.py)",
        "• อ่าน data/trusted/ ทั้งหมด (รวม 2024-07 หากมี) ใช้ RAM <= 205 MiB\n• data/curated/curated_monthly_summary.parquet / .csv (25 แถว)\n• data/sample/curated_trips_sample.parquet / .csv\n  (สุ่มแบ่งชั้นพาร์ทิชันละ 40 แถว x 25 = 1,000 แถวพอดี)",
        fc="#f0f9ff", ec="#0284c7", title_color="#0c4a6e"
    )

    draw_arrow(48.4, 12.0, 51.1, 12.0, label="ส่งต่อ", label_pos=(49.75, 13.8), color="#6d28d9", label_fs=7.6)

    draw_box(
        51.5, 6.5, 45.5, 11.0,
        "ขั้นที่ 5: วิเคราะห์คำตอบ & ตรวจ Regression (src/analyze.py)",
        "• กรองเฉพาะ H1 (2024-01..06) -> final_answer_by_taxi_type.csv\n  (ผู้ชนะ: FHVHV 120,864,668 ทริป / 80.76%)\n• ตรวจ H1 Regression เทียบ S1 (PASSED: 149,655,451, Diff = 0)\n• รัน Sensitivity 4 กรณี, วาดกราฟ และบันทึก logs/run_manifest_latest.json",
        fc="#faf5ff", ec="#9333ea", title_color="#4c1d95"
    )

    # Footer
    footer_box = patches.FancyBboxPatch(
        (3.0, 1.0), 94.0, 3.6,
        boxstyle="round,pad=0.25,rounding_size=0.8",
        facecolor="#f1f5f9", edgecolor="#94a3b8", linewidth=1.2, linestyle="--"
    )
    ax.add_patch(footer_box)
    ax.text(
        50.0, 2.8,
        "การต่อยอด Serving Layer บน Cloud (AWS S3 + Amazon Athena): รัน scripts/deploy_athena.py และ sql/athena_setup.sql เพื่อสร้าง External Table และ View บน Cloud",
        ha="center", va="center", fontsize=9.0, color="#334155", fontweight="bold"
    )

    plt.tight_layout()
    plt.savefig(output_png, dpi=300, bbox_inches="tight", pad_inches=0.2)
    plt.close()

    return output_png
