-- ==============================================================================
-- CS341 Big Data Engineering - Submission 2
-- Serving View บน Cloud (AWS S3 + Glue Catalog + Amazon Athena)
-- ==============================================================================
-- สคริปต์นี้สร้าง Database, External Table, และ Serving Views บน Amazon Athena
-- สำหรับเชื่อมต่อกับไฟล์ Curated Parquet บน S3 (อิงตามแนวคิด Views as a Serving Layer - Module 02)
--
-- ขั้นตอนการใช้งาน:
-- 1. อัปโหลด data/curated/curated_monthly_summary.parquet ขึ้น S3 Bucket
--    เช่น s3://<your-bucket-name>/data/curated/curated_monthly_summary.parquet
-- 2. แทนที่ <your-s3-bucket-name> ในสคริปต์นี้ด้วยชื่อ S3 Bucket จริงของคุณ
-- 3. เปิด Amazon Athena Query Editor แล้วรันคำสั่งตามลำดับด้านล่าง
-- ==============================================================================

-- 1. สร้าง Database สำหรับ Curated Data Zone
CREATE DATABASE IF NOT EXISTS nyc_taxi_curated
COMMENT 'Database สำหรับเก็บ Curated Serving Layer ของข้อมูล NYC Taxi H1 2024';

-- 2. สร้าง External Table ชี้ไปยังไฟล์ Parquet ใน S3 Bucket
CREATE EXTERNAL TABLE IF NOT EXISTS nyc_taxi_curated.curated_monthly_summary (
    taxi_type STRING COMMENT 'ประเภทแท็กซี่ (yellow, green, fhv, fhvhv)',
    batch_month STRING COMMENT 'เดือนของข้อมูล (YYYY-MM)',
    total_trips BIGINT COMMENT 'จำนวนทริปที่ผ่านเกณฑ์ Valid',
    avg_duration_seconds DOUBLE COMMENT 'ระยะเวลาทริปเฉลี่ย (วินาที)',
    min_pickup_ts TIMESTAMP COMMENT 'เวลาเริ่มต้นทริปแรกสุดในเดือน',
    max_pickup_ts TIMESTAMP COMMENT 'เวลาเริ่มต้นทริปล่าสุดในเดือน',
    null_pu_locations BIGINT COMMENT 'จำนวนแถวที่ PUlocationID เป็น NULL',
    null_do_locations BIGINT COMMENT 'จำนวนแถวที่ DOlocationID เป็น NULL'
)
STORED AS PARQUET
LOCATION 's3://<your-s3-bucket-name>/data/curated/'
TBLPROPERTIES (
    'parquet.compress' = 'ZSTD',
    'has_encrypted_data' = 'false'
);

-- 3. สร้าง Serving View: สรุปภาพรวมรายเดือนทุกประเภทแท็กซี่ (Module 02: Serving View)
CREATE OR REPLACE VIEW nyc_taxi_curated.v_curated_monthly_summary AS
SELECT
    taxi_type,
    batch_month,
    total_trips,
    avg_duration_seconds,
    min_pickup_ts,
    max_pickup_ts,
    null_pu_locations,
    null_do_locations
FROM nyc_taxi_curated.curated_monthly_summary;

-- 4. สร้าง Serving View: ตอบคำถามหลักและจัดอันดับส่วนแบ่งการตลาด H1 2024
--    (กรองเฉพาะ batch_month ระหว่าง 2024-01 ถึง 2024-06 ตามขอบเขตโจทย์ H1 2024)
CREATE OR REPLACE VIEW nyc_taxi_curated.v_h1_2024_ranking AS
WITH h1_data AS (
    SELECT
        taxi_type,
        SUM(total_trips) AS total_trips
    FROM nyc_taxi_curated.curated_monthly_summary
    WHERE batch_month BETWEEN '2024-01' AND '2024-06'
    GROUP BY taxi_type
),
total_calc AS (
    SELECT SUM(total_trips) AS grand_total FROM h1_data
)
SELECT
    h.taxi_type,
    h.total_trips,
    ROUND(CAST(h.total_trips AS DOUBLE) * 100.0 / CAST(t.grand_total AS DOUBLE), 2) AS market_share_percent,
    RANK() OVER (ORDER BY h.total_trips DESC) AS rank
FROM h1_data h, total_calc t;

-- ==============================================================================
-- ตัวอย่างคำสั่งคิวรีทดสอบบน Athena เพื่อตรวจผลลัพธ์
-- ==============================================================================
-- 1. ดูผลการจัดอันดับ H1 2024 (คำตอบธุรกิจหลัก):
-- SELECT * FROM nyc_taxi_curated.v_h1_2024_ranking ORDER BY rank ASC;

-- 2. ดูแนวโน้มปริมาณเที่ยววิ่งรายเดือน:
-- SELECT batch_month, taxi_type, total_trips
-- FROM nyc_taxi_curated.v_curated_monthly_summary
-- WHERE batch_month BETWEEN '2024-01' AND '2024-06'
-- ORDER BY batch_month, total_trips DESC;
