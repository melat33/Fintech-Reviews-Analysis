"""
2_data_pipeline/data_processing/data_cleaning.py

Purpose:
--------
Clean raw_reviews.csv:
    ✓ Text normalization
    ✓ Date standardization
    ✓ Duplicate removal
    ✓ Rating cleaning
    ✓ Output cleaned_reviews.csv (all banks + individual banks)
"""

import os
import sys
import pandas as pd
from dateutil import parser

# Ensure local imports work (normalize_text)
sys.path.append(os.path.dirname(__file__))
from preprocessing import normalize_text

# Paths
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RAW_PATH = os.path.join(ROOT, "2_data_pipeline", "outputs", "raw_reviews.csv")
PROCESSED_DIR = os.path.join(ROOT, "2_data_pipeline", "data", "processed")
COMBINED_CLEAN_PATH = os.path.join(PROCESSED_DIR, "all_clean_reviews.csv")


# ----------------------------------------------------
# DATE NORMALIZATION
# ----------------------------------------------------
def normalize_date(value):
    """Convert many date formats to YYYY-MM-DD."""
    if pd.isna(value) or value is None:
        return None

    try:
        return parser.parse(str(value)).date().isoformat()
    except Exception:
        # fallback: first 10 chars
        try:
            return str(value)[:10]
        except Exception:
            return None


# ----------------------------------------------------
# MAIN CLEANING FUNCTION
# ----------------------------------------------------
def clean():
    print("\n🧹 CLEANING RAW REVIEW DATA")
    print("────────────────────────────────────")

    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(f"❌ Raw file not found at: {RAW_PATH}")

    # Load raw data
    df = pd.read_csv(RAW_PATH, dtype=str)
    original_count = len(df)
    print(f"📄 Loaded raw reviews: {original_count:,}\n")

    # ---------------------------
    # REQUIRED COLUMNS CHECK
    # ---------------------------
    required_cols = ["review", "score", "at", "package_name", "bank_name", "review_id"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"❌ Missing required columns: {missing}")

    # ---------------------------
    # TEXT NORMALIZATION
    # ---------------------------
    print("🔤 Normalizing review text...")
    df["review_text"] = df["review"].fillna("").apply(normalize_text)
    df["reply_text"] = df.get("reply_text", "").fillna("").apply(normalize_text)

    # ---------------------------
    # RATING CLEANING
    # ---------------------------
    print("⭐ Cleaning rating scores...")
    df["rating"] = pd.to_numeric(df["score"], errors="coerce").astype("Int64")

    # ---------------------------
    # DATE CLEANING
    # ---------------------------
    print("📅 Normalizing dates...")
    df["review_date"] = df["at"].apply(normalize_date)
    df["reply_date"] = df.get("reply_date").apply(normalize_date) if "reply_date" in df.columns else None

    # ---------------------------
    # REMOVE EMPTY REVIEWS
    # ---------------------------
    print("\n🔍 Removing empty reviews...")
    before_drop = len(df)
    df = df[df["review_text"].str.strip() != ""]
    dropped_empty = before_drop - len(df)
    print(f"   → Dropped empty reviews: {dropped_empty}")

    # ---------------------------
    # DEDUPLICATION
    # ---------------------------
    print("🔁 Removing duplicate reviews...")
    df["dedupe_key"] = df["review_id"].fillna(df["package_name"] + "|" + df["review_text"].str[:200])
    before_dedupe = len(df)
    df = df.drop_duplicates(subset=["dedupe_key"])
    dropped_dupes = before_dedupe - len(df)
    print(f"   → Duplicates removed: {dropped_dupes}")

    # Cleanup helper column
    df.drop(columns=["dedupe_key", "review", "score", "at"], inplace=True, errors="ignore")

    # ---------------------------
    # SELECT FINAL COLUMNS
    # ---------------------------
    final_columns = [
        "review_id", "bank_name", "package_name",
        "user_name", "review_text", "rating", "review_date",
        "reply_text", "reply_date", "source"
    ]
    df = df[[c for c in final_columns if c in df.columns]]

    # ---------------------------
    # SAVE OUTPUTS
    # ---------------------------
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Save combined file
    df.to_csv(COMBINED_CLEAN_PATH, index=False, encoding="utf-8")
    print(f"\n✅ Combined cleaned data written to:\n   {COMBINED_CLEAN_PATH}")

    # Save individual bank files
    for bank in df['bank_name'].dropna().unique():
        bank_df = df[df['bank_name'] == bank]
        bank_file = os.path.join(PROCESSED_DIR, f"{bank.lower()}_clean_reviews.csv")
        bank_df.to_csv(bank_file, index=False, encoding="utf-8")
        print(f"   → Saved {len(bank_df):,} reviews for {bank} to {bank_file}")

    print(f"\n📊 Final total record count: {len(df):,}")
    print("🟢 Cleaning process completed.\n")

    return df


# ----------------------------------------------------
# RUN AS SCRIPT
# ----------------------------------------------------
if __name__ == "__main__":
    clean()
