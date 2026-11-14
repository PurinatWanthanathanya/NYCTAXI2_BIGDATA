"""
scripts/deploy_athena.py - Deploy curated outputs to AWS S3 & Athena.
CS341 Big Data Engineering - Submission 2
"""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
CURATED_PARQUET = BASE_DIR / "data" / "curated" / "curated_monthly_summary.parquet"
ATHENA_SQL_FILE = BASE_DIR / "sql" / "athena_setup.sql"


def check_aws_cli() -> bool:
    """Check if AWS CLI is installed and available in PATH."""
    return shutil.which("aws") is not None


def check_aws_credentials() -> bool:
    """Check if AWS credentials are valid."""
    try:
        res = subprocess.run(
            ["aws", "sts", "get-caller-identity"],
            capture_output=True,
            text=True,
            timeout=10
        )
        return res.returncode == 0
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Deploy Curated NYC Taxi Outputs to AWS S3 and Amazon Athena"
    )
    parser.add_argument(
        "--bucket",
        help="Target AWS S3 bucket name (e.g. 'my-cs341-taxi-bucket')"
    )
    parser.add_argument(
        "--prefix",
        default="data/curated",
        help="S3 folder prefix (default: 'data/curated')"
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region (default: 'us-east-1')"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate custom Athena DDL SQL without uploading to S3"
    )
    args = parser.parse_args()

    print("=" * 80)
    print("NYC TAXI PIPELINE - CLOUD SERVING DEPLOYMENT (AWS ATHENA / S3)")
    print("=" * 80)

    if not CURATED_PARQUET.exists():
        print(f"[!] Curated output not found at: {CURATED_PARQUET}")
        print("    Please run 'python run_pipeline.py' first.")
        sys.exit(1)

    print(f"[✓] Curated Parquet found: {CURATED_PARQUET}")

    has_cli = check_aws_cli()
    has_creds = check_aws_credentials() if has_cli else False

    print(f"[-] AWS CLI installed: {'YES' if has_cli else 'NO'}")
    print(f"[-] AWS Credentials active: {'YES' if has_creds else 'NO'}")

    if not args.bucket:
        print("\n" + "-" * 80)
        print("คำแนะนำสำหรับการ Deploy ขึ้น AWS Learner Lab:")
        print("-" * 80)
        print("1. รับค่า AWS Credentials จาก AWS Academy Learner Lab (ปุ่ม 'AWS Details')")
        print("2. ตั้งค่า Environment Variables ใน Terminal:")
        print("   set AWS_ACCESS_KEY_ID=<your-access-key-id>")
        print("   set AWS_SECRET_ACCESS_KEY=<your-secret-access-key>")
        print("   set AWS_SESSION_TOKEN=<your-session-token>")
        print("   set AWS_DEFAULT_REGION=us-east-1")
        print("3. รันคำสั่ง:")
        print("   python scripts/deploy_athena.py --bucket <your-bucket-name>")
        print("\nหรือรันแบบ Dry-run เพื่อดูคำสั่ง SQL ที่เตรียมไว้สำหรับ S3 Bucket ของคุณ:")
        print("   python scripts/deploy_athena.py --bucket <your-bucket-name> --dry-run")
        print("-" * 80)
        return

    s3_uri = f"s3://{args.bucket}/{args.prefix}/"
    s3_target_file = f"{s3_uri}curated_monthly_summary.parquet"

    # Generate custom SQL with actual bucket name
    if ATHENA_SQL_FILE.exists():
        sql_template = ATHENA_SQL_FILE.read_text(encoding="utf-8")
        custom_sql = sql_template.replace("s3://<your-s3-bucket-name>/data/curated/", s3_uri)
        generated_sql_path = BASE_DIR / "sql" / f"athena_setup_{args.bucket}.sql"
        generated_sql_path.write_text(custom_sql, encoding="utf-8")
        print(f"[✓] Generated customized Athena SQL script: {generated_sql_path}")

    if args.dry_run:
        print(f"\n[DRY RUN] Target S3 URI: {s3_target_file}")
        print("[DRY RUN] Copy the generated SQL script into the Athena Query Editor to create the tables/views.")
        return

    if not has_creds:
        print("\n[!] Error: No active AWS credentials found in this session.")
        print("    Please set AWS credentials or use --dry-run.")
        sys.exit(1)

    print(f"\n[+] Uploading {CURATED_PARQUET.name} to {s3_target_file}...")
    upload_cmd = ["aws", "s3", "cp", str(CURATED_PARQUET), s3_target_file, "--region", args.region]
    res = subprocess.run(upload_cmd, capture_output=True, text=True)
    if res.returncode == 0:
        print(f"[✓] Upload successful: {s3_target_file}")
        print("\n[✓] Next step: Open Amazon Athena in AWS Console and execute:")
        print(f"    File: {generated_sql_path}")
    else:
        print(f"[!] Upload failed: {res.stderr}")
        sys.exit(res.returncode)


if __name__ == "__main__":
    main()
