"""
2_data_pipeline/data_processing/data_cleaning.py
Clean raw_reviews.csv with better debugging
"""

import os
import sys
import pandas as pd
from dateutil import parser

# Ensure local imports work (normalize_text)
sys.path.append(os.path.dirname(__file__))

try:
    from preprocessing import normalize_text
except ImportError:
    print("❌ Could not import normalize_text from preprocessing.py")
    sys.exit(1)

# ----------------------------------------------------
# Paths
# ----------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RAW_PATH = os.path.join(ROOT, "2_data_pipeline", "data", "raw", "all_reviews.csv")
PROCESSED_DIR = os.path.join(ROOT, "2_data_pipeline", "data", "processed")
COMBINED_CLEAN_PATH = os.path.join(PROCESSED_DIR, "all_clean_reviews.csv")
os.makedirs(PROCESSED_DIR, exist_ok=True)

# ----------------------------------------------------
# Load expected banks from config
# ----------------------------------------------------
def get_expected_banks():
    """Get list of expected banks from config"""
    try:
        import yaml
        config_path = os.path.join(ROOT, "3_configuration", "config.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return list(cfg.get("package_map", {}).values())
    except Exception as e:
        print(f"⚠️ Could not load config: {e}")
        return []

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
        try:
            return str(value)[:10]
        except Exception:
            return None

# ----------------------------------------------------
# MAIN CLEANING FUNCTION
# ----------------------------------------------------
def clean():
    print("\n🧹 CLEANING RAW REVIEW DATA")
    print("=" * 50)

    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(f"❌ Raw file not found at: {RAW_PATH}")

    # Load raw data
    try:
        df = pd.read_csv(RAW_PATH, dtype=str)
        original_count = len(df)
        print(f"📄 Loaded raw reviews: {original_count:,}")
        
        # DEBUG: Show banks in raw data
        print("\n🔍 BANKS IN RAW DATA:")
        bank_counts = df['bank_name'].value_counts()
        for bank, count in bank_counts.items():
            print(f"   • {bank}: {count} reviews")
            
        # Check for missing expected banks
        expected_banks = get_expected_banks()
        if expected_banks:
            missing_banks = set(expected_banks) - set(bank_counts.index)
            if missing_banks:
                print(f"\n⚠️  MISSING BANKS IN RAW DATA: {list(missing_banks)}")
        
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        return None

    # Continue with existing cleaning logic...
    # [Rest of your existing cleaning code remains the same]

    # ---------------------------
    # REQUIRED COLUMNS CHECK
    # ---------------------------
    required_cols = ["review", "score", "at", "package_name", "bank_name", "review_id"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"❌ Missing required columns: {missing}")
        return None

    # ---------------------------
    # TEXT NORMALIZATION
    # ---------------------------
    print("🔤 Normalizing review text...")
    df["review_text"] = df["review"].fillna("").apply(normalize_text)
    if "reply_text" in df.columns:
        df["reply_text"] = df["reply_text"].fillna("").apply(normalize_text)

    # ---------------------------
    # RATING CLEANING
    # ---------------------------
    print("⭐ Cleaning rating scores...")
    df["rating"] = pd.to_numeric(df["score"], errors="coerce")
    invalid_ratings = df["rating"].isna().sum()
    if invalid_ratings > 0:
        print(f"   → Found {invalid_ratings} invalid ratings")

    # ---------------------------
    # DATE CLEANING
    # ---------------------------
    print("📅 Normalizing dates...")
    df["review_date"] = df["at"].apply(normalize_date)
    invalid_dates = df["review_date"].isna().sum()
    if invalid_dates > 0:
        print(f"   → Found {invalid_dates} invalid dates")
    
    if "reply_date" in df.columns:
        df["reply_date"] = df["reply_date"].apply(normalize_date)

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
    print("\n💾 Saving individual bank files...")
    for bank in df['bank_name'].dropna().unique():
        bank_df = df[df['bank_name'] == bank]
        bank_file = os.path.join(PROCESSED_DIR, f"{bank.lower().replace(' ', '_')}_clean_reviews.csv")
        bank_df.to_csv(bank_file, index=False, encoding="utf-8")
        print(f"   → {bank}: {len(bank_df):,} reviews")

    # Final summary with bank comparison
    print(f"\n📊 CLEANING SUMMARY:")
    print(f"   Original records: {original_count:,}")
    print(f"   Final records: {len(df):,}")
    print(f"   Records removed: {original_count - len(df):,}")
    
    if expected_banks:
        final_banks = set(df['bank_name'].unique())
        still_missing = set(expected_banks) - final_banks
        if still_missing:
            print(f"   ⚠️  Still missing banks: {list(still_missing)}")

    print(f"   Output directory: {PROCESSED_DIR}")
    print("\n🟢 Cleaning process completed!\n")

    return df

if __name__ == "__main__":
    clean()