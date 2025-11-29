"""
2_data_pipeline/data_processing/quality_checks.py
Run lightweight QA checks on cleaned_reviews.csv
"""

import os
import pandas as pd

# ----------------------------------------------------
# Paths - CORRECTED to use processed directory
# ----------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CLEAN_PATH = os.path.join(ROOT, "2_data_pipeline", "data", "processed", "all_clean_reviews.csv")

def run_checks():
    print("\n🔍 DATA QUALITY CHECKS - CLEANED DATA")
    print("=" * 50)

    if not os.path.exists(CLEAN_PATH):
        raise FileNotFoundError(f"❌ Clean file not found at: {CLEAN_PATH}\n➡️ Run data_cleaning.py first.")
    
    # Load cleaned data
    try:
        df = pd.read_csv(CLEAN_PATH, dtype=str)
        total = len(df)
        print(f"📊 Total cleaned records: {total:,}")
        print(f"📁 Source: {CLEAN_PATH}\n")
    except Exception as e:
        print(f"❌ Error loading cleaned data: {e}")
        return None
    
    # ---------------------------
    # BASIC STRUCTURE CHECKS
    # ---------------------------
    print("📋 DATA STRUCTURE:")
    print(f"   Columns: {list(df.columns)}")
    
    required_cols = ["bank_name", "review_text", "rating", "review_date"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"❌ Missing required columns: {missing_cols}")
    else:
        print("✅ All required columns present")
    
    # ---------------------------
    # BANK DISTRIBUTION
    # ---------------------------
    print("\n🏦 RECORDS BY BANK:")
    bank_counts = df['bank_name'].value_counts()
    for bank, count in bank_counts.items():
        percentage = (count / total) * 100
        print(f"   {bank}: {count:,} ({percentage:.1f}%)")
    
    # ---------------------------
    # RATING DISTRIBUTION
    # ---------------------------
    if 'rating' in df.columns:
        print("\n⭐ RATING DISTRIBUTION:")
        try:
            ratings = pd.to_numeric(df['rating'], errors='coerce')
            rating_counts = ratings.value_counts(dropna=False).sort_index()
            for rating, count in rating_counts.items():
                percentage = (count / total) * 100
                if pd.isna(rating):
                    print(f"   Missing/Invalid: {count:,} ({percentage:.1f}%)")
                else:
                    print(f"   {int(rating)} stars: {count:,} ({percentage:.1f}%)")
            
            # Rating statistics
            valid_ratings = ratings.dropna()
            if len(valid_ratings) > 0:
                print(f"   Average rating: {valid_ratings.mean():.2f}/5")
        except Exception as e:
            print(f"   Error analyzing ratings: {e}")
    
    # ---------------------------
    # DATE QUALITY
    # ---------------------------
    if 'review_date' in df.columns:
        miss_dates = df['review_date'].isna().sum()
        print(f"\n📅 DATE QUALITY:")
        print(f"   Missing review_date: {miss_dates} ({(miss_dates/total*100):.2f}%)")
        
        # Check date range
        try:
            valid_dates = pd.to_datetime(df['review_date'], errors='coerce')
            date_range = valid_dates.dropna()
            if len(date_range) > 0:
                print(f"   Date range: {date_range.min().strftime('%Y-%m-%d')} to {date_range.max().strftime('%Y-%m-%d')}")
        except:
            pass
    
    # ---------------------------
    # REVIEW CONTENT QUALITY
    # ---------------------------
    if 'review_text' in df.columns:
        print(f"\n📝 REVIEW CONTENT ANALYSIS:")
        
        # Review length analysis
        df['review_length'] = df['review_text'].str.len()
        avg_length = df['review_length'].mean()
        median_length = df['review_length'].median()
        print(f"   Average length: {avg_length:.1f} chars")
        print(f"   Median length: {median_length:.1f} chars")
        
        # Very short reviews
        very_short = df[df['review_length'] < 10]
        print(f"   Very short reviews (<10 chars): {len(very_short):,}")
        
        if len(very_short) > 0:
            print(f"\n   Sample very short reviews:")
            sample = very_short[['bank_name','review_text']].head(3)
            for idx, row in sample.iterrows():
                print(f"     - {row['bank_name']}: '{row['review_text']}'")
        
        # Empty reviews (should be 0 after cleaning)
        empty_reviews = df[df['review_text'].isna() | (df['review_text'].str.strip() == '')]
        print(f"   Empty reviews: {len(empty_reviews):,}")
    
    # ---------------------------
    # DATA COMPLETENESS
    # ---------------------------
    print(f"\n📈 DATA COMPLETENESS:")
    completeness = df.notna().mean() * 100
    for col, percent in completeness.items():
        print(f"   {col}: {percent:.1f}% complete")
    
    overall_completeness = df.notna().mean().mean() * 100
    print(f"   Overall: {overall_completeness:.1f}% complete")
    
    print(f"\n✅ Quality checks completed successfully!")
    return df

if __name__ == "__main__":
    run_checks()