# ======================================================
#   TASK 2 – SENTIMENT + THEMATIC ANALYSIS PIPELINE
#   PER-BANK SENTIMENT ANALYSIS VERSION
# ======================================================

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# -------------------------------
#  IMPORT SENTIMENT MODULES
# -------------------------------
from sentiment_analysis.lexicon_vader import vader_sentiment
from sentiment_analysis.textblob_sentiment import textblob_sentiment
from sentiment_analysis.ml_sentiment_classifier import train_ml_model, predict_ml_sentiment
from sentiment_analysis.bert_sentiment import bert_sentiment
from sentiment_analysis.ensemble_sentiment import ensemble_sentiment

# -------------------------------
#  IMPORT THEMATIC MODULES
# -------------------------------
from thematic_analysis.keyword_extraction import extract_keywords
from thematic_analysis.theme_clustering import cluster_themes

# -------------------------------
#  PATHS
# -------------------------------
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "2_data_pipeline", "data", "processed")
ALL_CLEAN_PATH = os.path.join(PROCESSED_DIR, "all_clean_reviews.csv")

print("📂 PROJECT ROOT:", PROJECT_ROOT)
print("📂 LOADING CLEAN DATA FROM:", ALL_CLEAN_PATH)

# -------------------------------
#  LOAD CLEAN DATA
# -------------------------------
df_clean = pd.read_csv(ALL_CLEAN_PATH)
print(f"✅ Loaded {len(df_clean):,} cleaned reviews")

# Get unique banks for per-bank processing
unique_banks = df_clean['bank_name'].unique()
print(f"🏦 Found {len(unique_banks)} banks: {list(unique_banks)}")

# -------------------------------
#  SENTIMENT ANALYSIS - PER BANK
# -------------------------------
bank_sentiment_dfs = []

for bank in unique_banks:
    print(f"\n{'='*50}")
    print(f"🔍 PROCESSING BANK: {bank}")
    print(f"{'='*50}")
    
    # Filter data for current bank
    bank_df = df_clean[df_clean['bank_name'] == bank].copy()
    print(f"📊 {len(bank_df):,} reviews for {bank}")
    
    # Run sentiment analysis pipeline for this bank
    print("   Running VADER sentiment...")
    bank_df = vader_sentiment(bank_df)
    
    print("   Running TextBlob sentiment...")
    bank_df = textblob_sentiment(bank_df)
    
    print("   Training ML model for this bank...")
    ml_model, ml_vectorizer = train_ml_model(bank_df)
    bank_df = predict_ml_sentiment(bank_df, ml_model, ml_vectorizer)
    
    print("   Running BERT sentiment...")
    bank_df = bert_sentiment(bank_df)
    
    print("   Applying ensemble voting...")
    bank_df = ensemble_sentiment(bank_df)
    
    # Thematic analysis for this bank
    print("   Extracting keywords...")
    bank_keywords = extract_keywords(bank_df)
    
    print("   Clustering themes...")
    bank_df = cluster_themes(bank_df)
    
    # Store the processed bank data
    bank_sentiment_dfs.append(bank_df)
    
    print(f"✅ Completed sentiment analysis for {bank}")
    print(f"🔥 Top keywords for {bank}: {bank_keywords[:5]}")

# Combine all bank data back together
df_final = pd.concat(bank_sentiment_dfs, ignore_index=True)
print(f"\n🎉 Combined {len(bank_sentiment_dfs)} banks into final dataset")

# -------------------------------
#  SAVE PER-BANK SENTIMENT FILES
# -------------------------------
print("\n💾 SAVING PER-BANK SENTIMENT FILES:")
print("-" * 40)

for bank in unique_banks:
    bank_df = df_final[df_final['bank_name'] == bank].copy()
    
    # Save individual bank sentiment file
    bank_filename = f"{bank.lower().replace(' ', '_')}_sentiment_reviews.csv"
    bank_filepath = os.path.join(PROCESSED_DIR, bank_filename)
    bank_df.to_csv(bank_filepath, index=False)
    
    # Calculate bank-specific stats
    total_reviews = len(bank_df)
    positive_reviews = len(bank_df[bank_df['ensemble_label'] == 'positive'])
    negative_reviews = len(bank_df[bank_df['ensemble_label'] == 'negative'])
    neutral_reviews = len(bank_df[bank_df['ensemble_label'] == 'neutral'])
    
    print(f"🏦 {bank}:")
    print(f"   📁 {bank_filename}")
    print(f"   📊 {total_reviews:,} reviews")
    print(f"   👍 {positive_reviews} positive ({positive_reviews/total_reviews*100:.1f}%)")
    print(f"   👎 {negative_reviews} negative ({negative_reviews/total_reviews*100:.1f}%)")
    print(f"   😐 {neutral_reviews} neutral ({neutral_reviews/total_reviews*100:.1f}%)")

# Also save the combined file (optional)
combined_output_path = os.path.join(PROCESSED_DIR, "all_sentiment_reviews.csv")
df_final.to_csv(combined_output_path, index=False)
print(f"\n💾 Combined file saved: {combined_output_path}")