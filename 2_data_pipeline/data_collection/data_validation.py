"""
2_data_pipeline/data_collection/data_validation.py
Validates raw review files from data/raw/
"""

import os
import pandas as pd

# ---------------------------------------------------------
# Paths - CORRECTED to use data/raw directory
# ---------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Updated to point to the raw directory
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

    print(f"📊 Total records: {len(df)}")
    print("\n❓ Missing values:")
    print(df.isna().sum())

    if "bank_name" in df.columns:
        print("\n🏦 Bank distribution:")
        print(df["bank_name"].value_counts(dropna=False))

    if "at" in df.columns:
        parsed = pd.to_datetime(df["at"], errors="coerce")
        invalid_dates = parsed.isna().sum()
        print(f"\n📅 Unparseable dates: {invalid_dates}")
        if invalid_dates > 0:
            print("   Sample invalid dates:")
            invalid_samples = df[parsed.isna()]["at"].head(3).tolist()
            for sample in invalid_samples:
                print(f"     - {sample}")

    line()
    return df


# ---------------------------------------------------------
# Main validation
# ---------------------------------------------------------
def validate_all():
    line()
    print("🔍 RAW DATA VALIDATION REPORT")
    print(f"📁 Directory: {RAW_DIR}")
    line()

    # Check if directory exists
    if not os.path.exists(RAW_DIR):
        print(f"❌ Directory not found: {RAW_DIR}")
        print("➡️ Run the scraper first to create the directory and files.")
        return

    # Validate combined raw file
    validate_file(ALL_RAW_PATH)

    # Validate each bank file
    print("\n🔎 Validating per-bank raw files...")
    bank_files_found = 0
    for fname in os.listdir(RAW_DIR):
        if fname.endswith("_reviews.csv") and fname != "all_reviews.csv":
            validate_file(os.path.join(RAW_DIR, fname))
            bank_files_found += 1
    
    if bank_files_found == 0:
        print("ℹ️  No individual bank files found in the directory.")

    print("\n✅ Validation completed successfully.")


if __name__ == "__main__":
    validate_all()