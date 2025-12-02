"""
DRIVER ANALYSIS: Identifying key factors influencing customer sentiment
"""

import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import nltk
from nltk.corpus import stopwords
import re
import os
import sys

# Get the project root directory
def get_project_root():
    """Get the project root directory"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up two levels from insights_analysis folder
    project_root = os.path.dirname(os.path.dirname(current_dir))
    return project_root

def load_and_prepare_data():
    """Load cleaned data and sentiment analysis results"""
    
    ROOT = get_project_root()
    print(f"Project root: {ROOT}")
    
    # Define paths using absolute paths
    processed_dir = os.path.join(ROOT, "2_data_pipeline", "data", "processed")
    sentiment_dir = os.path.join(ROOT, "sentiment_analysis")
    
    # Check if directories exist
    if not os.path.exists(processed_dir):
        print(f"❌ Processed data directory not found: {processed_dir}")
        # Try alternative paths
        alt_paths = [
            os.path.join(ROOT, "data", "processed"),
            os.path.join(ROOT, "processed"),
            "2_data_pipeline/data/processed",
            "data/processed"
        ]
        
        for alt_path in alt_paths:
            if os.path.exists(alt_path):
                processed_dir = alt_path
                print(f"✓ Found alternative path: {processed_dir}")
                break
    
    # Load clean reviews
    clean_data_path = os.path.join(processed_dir, "all_clean_reviews.csv")
    print(f"Looking for clean data at: {clean_data_path}")
    
    if not os.path.exists(clean_data_path):
        print(f"❌ Clean data file not found at: {clean_data_path}")
        print("Available files in directory:")
        if os.path.exists(processed_dir):
            for file in os.listdir(processed_dir):
                print(f"  - {file}")
        
        # Try to find the file
        possible_files = [
            "all_clean_reviews.csv",
            "clean_reviews.csv", 
            "processed_reviews.csv",
            "reviews_clean.csv"
        ]
        
        for file in possible_files:
            test_path = os.path.join(processed_dir, file)
            if os.path.exists(test_path):
                clean_data_path = test_path
                print(f"✓ Found alternative file: {file}")
                break
    
    df = pd.read_csv(clean_data_path)
    print(f"✓ Loaded {len(df)} clean reviews from {clean_data_path}")
    
    # Load sentiment analysis
    sentiment_path = os.path.join(sentiment_dir, "sentiment_results.csv")
    if not os.path.exists(sentiment_path):
        print(f"❌ Sentiment results not found at: {sentiment_path}")
        # Try alternative sentiment file names
        possible_sentiment_files = [
            "sentiment_analysis_results.csv",
            "sentiment_scores.csv",
            "all_sentiment_results.csv",
            "final_sentiment.csv"
        ]
        
        for file in possible_sentiment_files:
            test_path = os.path.join(sentiment_dir, file)
            if os.path.exists(test_path):
                sentiment_path = test_path
                print(f"✓ Found alternative sentiment file: {file}")
                break
        else:
            # If still not found, check in root
            sentiment_path = os.path.join(ROOT, "sentiment_results.csv")
    
    print(f"Looking for sentiment data at: {sentiment_path}")
    
    if os.path.exists(sentiment_path):
        sentiment_df = pd.read_csv(sentiment_path)
        print(f"✓ Loaded sentiment data for {len(sentiment_df)} reviews")
    else:
        print("⚠️  Sentiment file not found. Creating sentiment data from scratch...")
        # Create basic sentiment data if file doesn't exist
        sentiment_df = pd.DataFrame({
            'review_id': df['review_id'],
            'final_sentiment': 'neutral',
            'sentiment_score': 0.0
        })
    
    # Merge data
    if 'review_id' in df.columns and 'review_id' in sentiment_df.columns:
        df = df.merge(sentiment_df[['review_id', 'final_sentiment', 'sentiment_score']], 
                      on='review_id', how='left')
    else:
        print("⚠️  Could not merge - review_id column missing")
        print(f"Data columns: {df.columns.tolist()}")
        print(f"Sentiment columns: {sentiment_df.columns.tolist()}")
        
        # Try to find common column
        common_cols = set(df.columns) & set(sentiment_df.columns)
        if common_cols:
            common_col = list(common_cols)[0]
            print(f"Using common column: {common_col}")
            df = df.merge(sentiment_df, on=common_col, how='left')
        else:
            # Add sentiment scores directly
            df['final_sentiment'] = 'neutral'
            df['sentiment_score'] = 0.0
    
    # Add sentiment category
    if 'sentiment_score' in df.columns:
        df['sentiment_category'] = pd.cut(df['sentiment_score'], 
                                          bins=[-1, -0.1, 0.1, 1],
                                          labels=['negative', 'neutral', 'positive'])
    else:
        df['sentiment_category'] = 'neutral'
        df['sentiment_score'] = 0.0
    
    print(f"✓ Data prepared. Final shape: {df.shape}")
    print(f"✓ Columns: {df.columns.tolist()}")
    
    return df, sentiment_df