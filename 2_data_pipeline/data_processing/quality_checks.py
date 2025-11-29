"""
2_data_pipeline/data_processing/quality_checks.py
Run lightweight QA checks on cleaned_reviews.csv
"""

import os
import sys
import pandas as pd

# Add the current directory to Python path to fix import issues
sys.path.append(os.path.dirname(__file__))

from preprocessing import normalize_text

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CLEAN_PATH = os.path.join(ROOT, "2_data_pipeline", "data", "processed", "all_clean_reviews.csv")

def run_checks():
    if not os.path.exists(CLEAN_PATH):
        raise FileNotFoundError("Clean file not found. Run data_cleaning.py first.")
    
    df = pd.read_csv(CLEAN_PATH, dtype=str)
    total = len(df)
    print(f"Total cleaned records: {total}")
    
    print("\nRecords by bank:")
    print(df['bank_name'].value_counts())
    
    if 'rating' in df.columns:
        print("\nRating distribution:")
        print(df['rating'].value_counts(dropna=False).sort_index())
    
    miss_dates = df['review_date'].isna().sum() if 'review_date' in df.columns else 0
    print(f"\nMissing review_date: {miss_dates} ({(miss_dates/total*100):.2f}%)")
    
    df['len'] = df['review_text'].str.len()
    very_short = df[df['len'] < 10]
    print(f"\nVery short reviews (<10 chars): {len(very_short)}")
    if len(very_short) > 0:
        print("\nSample very short reviews:")
        print(very_short[['bank_name','review_text']].head(5).to_string(index=False))
    
    return df

if __name__ == "__main__":
    run_checks()
