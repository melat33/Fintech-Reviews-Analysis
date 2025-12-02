"""
SIMPLIFIED ANALYSIS FOR TASK 4
This file contains simplified analysis functions
"""

import pandas as pd
import numpy as np
from collections import Counter
import re

def perform_simplified_analysis(df):
    """Perform simplified analysis on the data"""
    
    print("📊 Performing simplified analysis...")
    
    results = {}
    
    # Basic metrics
    results['total_reviews'] = len(df)
    results['avg_rating'] = float(df['rating'].mean())
    results['avg_sentiment'] = float(df['sentiment_score'].mean()) if 'sentiment_score' in df.columns else 0.0
    
    # Sentiment distribution
    if 'sentiment_category' in df.columns:
        sentiment_counts = df['sentiment_category'].value_counts()
        results['sentiment_distribution'] = sentiment_counts.to_dict()
    else:
        results['sentiment_distribution'] = {'neutral': len(df)}
    
    # Bank-wise metrics
    bank_metrics = []
    for bank in df['bank_name'].unique():
        bank_data = df[df['bank_name'] == bank]
        
        metrics = {
            'bank': bank,
            'total_reviews': int(len(bank_data)),
            'avg_rating': float(bank_data['rating'].mean()),
        }
        
        if 'sentiment_category' in bank_data.columns:
            metrics['positive_pct'] = float((bank_data['sentiment_category'] == 'positive').mean() * 100)
            metrics['negative_pct'] = float((bank_data['sentiment_category'] == 'negative').mean() * 100)
        
        if 'sentiment_score' in bank_data.columns:
            metrics['avg_sentiment'] = float(bank_data['sentiment_score'].mean())
        
        bank_metrics.append(metrics)
    
    results['bank_metrics'] = bank_metrics
    
    # Top positive keywords
    if 'sentiment_category' in df.columns and 'review_text' in df.columns:
        positive_reviews = df[df['sentiment_category'] == 'positive']
        if len(positive_reviews) > 0:
            all_text = ' '.join(positive_reviews['review_text'].fillna('').astype(str))
            words = re.findall(r'\b\w+\b', all_text.lower())
            word_freq = Counter([w for w in words if len(w) > 3 and w not in ['bank', 'service', 'customer', 'would', 'like']])
            results['top_positive_words'] = dict(word_freq.most_common(10))
    
    # Top negative keywords
    if 'sentiment_category' in df.columns and 'review_text' in df.columns:
        negative_reviews = df[df['sentiment_category'] == 'negative']
        if len(negative_reviews) > 0:
            all_text = ' '.join(negative_reviews['review_text'].fillna('').astype(str))
            words = re.findall(r'\b\w+\b', all_text.lower())
            word_freq = Counter([w for w in words if len(w) > 3 and w not in ['bank', 'service', 'customer', 'would', 'like']])
            results['top_negative_words'] = dict(word_freq.most_common(10))
    
    # Common pain points
    complaint_patterns = {
        'slow_service': ['slow', 'wait', 'delay', 'long', 'time'],
        'fees_charges': ['fee', 'charge', 'cost', 'expensive', 'money'],
        'technical_issues': ['app', 'online', 'mobile', 'website', 'error'],
        'staff_behavior': ['staff', 'rude', 'unhelpful', 'attitude', 'employee'],
        'account_issues': ['account', 'password', 'login', 'access', 'blocked']
    }
    
    pain_points = {}
    if 'sentiment_category' in df.columns and 'review_text' in df.columns:
        negative_reviews = df[df['sentiment_category'] == 'negative']
        for pattern_name, keywords in complaint_patterns.items():
            pattern = '|'.join(keywords)
            count = negative_reviews['review_text'].str.contains(pattern, case=False, na=False).sum()
            if count > 0:
                pain_points[pattern_name] = {
                    'count': int(count),
                    'percentage': float((count / len(negative_reviews)) * 100) if len(negative_reviews) > 0 else 0.0,
                    'keywords': keywords[:3]
                }
    
    results['pain_points'] = pain_points
    
    print(f"   ✓ Analyzed {len(df)} reviews")
    print(f"   ✓ Processed {len(bank_metrics)} banks")
    print(f"   ✓ Identified {len(pain_points)} pain points")
    
    return results