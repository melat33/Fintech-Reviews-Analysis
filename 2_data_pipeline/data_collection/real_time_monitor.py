"""
2_data_pipeline/data_collection/real_time_monitor.py

Purpose:
--------
Real-time lightweight monitoring tool to check:
    ✓ Current total review count
    ✓ Reviews per package_name
    ✓ Reviews per bank_name (if available)
    ✓ Duplicate checks
    ✓ Last updated timestamp
    ✓ Early warning for missing columns

This helps track scraper progress without loading heavy notebooks.
"""

import os
import pandas as pd
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RAW_PATH = os.path.join(ROOT, "2_data_pipeline", "outputs", "raw_reviews.csv")

def report():
    print("\n📌 REAL-TIME SCRAPER MONITOR")
    print("───────────────────────────────────────────────")

    if not os.path.exists(RAW_PATH):
        print("❌ No raw_reviews.csv file found.")
        print("➡️ Run the scraper before monitoring.\n")
        return

    # Load dataset
    try:
        df = pd.read_csv(RAW_PATH, dtype=str)
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        return

    print(f"📄 Loaded file: raw_reviews.csv")
    print(f"🧮 Total reviews collected so far: {len(df):,}")

    # Track last modified time of the CSV file
    last_update = datetime.fromtimestamp(os.path.getmtime(RAW_PATH))
    print(f"⏱ Last updated: {last_update.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # --------------------------
    # CHECK REQUIRED COLUMNS
    # --------------------------
    required_cols = ["package_name", "review", "bank_name", "at"]
    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        print(f"⚠️ Missing columns: {missing}\n")
    else:
        print("✔ All required columns present.\n")

    # --------------------------
    # COUNTS PER PACKAGE
    # --------------------------
    if "package_name" in df.columns:
        print("📦 Review counts per mobile app package:")
        pkg_counts = df["package_name"].value_counts()
        for pkg, cnt in pkg_counts.items():
            print(f"  • {pkg}: {cnt:,}")
        print()

    # --------------------------
    # COUNTS PER BANK
    # --------------------------
    if "bank_name" in df.columns:
        print("🏦 Review counts per bank:")
        bank_counts = df["bank_name"].value_counts()
        for bank, cnt in bank_counts.items():
            print(f"  • {bank}: {cnt:,}")
        print()

    # --------------------------
    # DUPLICATE CHECK
    # --------------------------
    if "review" in df.columns:
        dup_count = df["review"].duplicated().sum()
        print(f"🔁 Possible duplicate review texts: {dup_count:,}")
    else:
        print("⚠️ Cannot check duplicates — 'review' column missing.")

    print("\n✅ Monitoring complete.\n")


if __name__ == "__main__":
    report()
