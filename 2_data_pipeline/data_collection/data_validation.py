"""
2_data_pipeline/data_collection/data_validation.py
Validates raw review files from data/raw/
"""

import os
import pandas as pd

# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

RAW_DIR = os.path.join(ROOT, "2_data_pipeline", "data", "raw")
ALL_RAW_PATH = os.path.join(RAW_DIR, "all_reviews.csv")

# ---------------------------------------------------------
# Utility: Pretty print
# ---------------------------------------------------------
def line():
    print("=" * 70)


# ---------------------------------------------------------
# Validate a single CSV file
# ---------------------------------------------------------
def validate_file(path: str):
    if not os.path.exists(path):
        print(f"❌ File not found: {path}")
        return None

    print(f"\n📌 Validating: {os.path.basename(path)}")
    df = pd.read_csv(path, dtype=str)

    print(f"Total records: {len(df)}")
    print("\nMissing values:")
    print(df.isna().sum())

    if "bank_name" in df.columns:
        print("\nBank distribution:")
        print(df["bank_name"].value_counts(dropna=False))

    if "at" in df.columns:
        parsed = pd.to_datetime(df["at"], errors="coerce")
        print(f"\nUnparseable dates: {parsed.isna().sum()}")

    line()
    return df


# ---------------------------------------------------------
# Main validation
# ---------------------------------------------------------
def validate_all():

    line()
    print("🔍 RAW DATA VALIDATION REPORT")
    line()

    # Validate combined raw file
    validate_file(ALL_RAW_PATH)

    # Validate each bank file
    print("\n🔎 Validating per-bank raw files...")
    for fname in os.listdir(RAW_DIR):
        if fname.endswith("_reviews.csv") and fname != "all_reviews.csv":
            validate_file(os.path.join(RAW_DIR, fname))

    print("\n🎉 Validation completed successfully.")


if __name__ == "__main__":
    validate_all()
