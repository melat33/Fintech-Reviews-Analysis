"""
PAIN POINT IDENTIFICATION: Advanced analysis of customer complaints
"""

import pandas as pd
import numpy as np
from textblob import TextBlob
import re
from collections import Counter

def analyze_emotional_intensity(text):
    """Analyze emotional intensity of complaints"""
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity
    subjectivity = blob.sentiment.subjectivity
    
    # Calculate intensity score (more negative + more subjective = more intense)
    intensity = abs(polarity) * (1 + subjectivity)
    return intensity

def identify_severe_complaints(df, top_n=20):
    """Identify most severe complaints based on multiple factors"""
    
    negative_reviews = df[df['sentiment_category'] == 'negative'].copy()
    
    if len(negative_reviews) == 0:
        return pd.DataFrame()
    
    # Calculate complaint severity score
    negative_reviews['emotional_intensity'] = negative_reviews['cleaned_content'].apply(analyze_emotional_intensity)
    negative_reviews['word_count'] = negative_reviews['cleaned_content'].apply(lambda x: len(str(x).split()))
    negative_reviews['exclamation_count'] = negative_reviews['cleaned_content'].apply(lambda x: str(x).count('!'))
    
    # Normalize scores
    for col in ['emotional_intensity', 'word_count', 'exclamation_count']:
        negative_reviews[f'{col}_norm'] = (negative_reviews[col] - negative_reviews[col].min()) / \
                                         (negative_reviews[col].max() - negative_reviews[col].min() + 1e-10)
    
    # Calculate composite severity score
    negative_reviews['severity_score'] = (
        negative_reviews['emotional_intensity_norm'] * 0.4 +
        (1 - negative_reviews['rating']/5) * 0.3 +  # Lower rating = more severe
        negative_reviews['word_count_norm'] * 0.2 +
        negative_reviews['exclamation_count_norm'] * 0.1
    )
    
    # Get top severe complaints
    severe_complaints = negative_reviews.nlargest(top_n, 'severity_score')[
        ['bank_name', 'rating', 'sentiment_score', 'severity_score', 'cleaned_content']
    ]
    
    return severe_complaints

def categorize_complaint_themes(text):
    """Categorize complaints into themes"""
    
    text_lower = str(text).lower()
    
    # Theme patterns
    themes = {
        'digital_banking': ['app', 'online', 'mobile', 'website', 'internet', 'digital'],
        'customer_service': ['staff', 'rude', 'helpful', 'service', 'employee', 'attitude'],
        'transaction_issues': ['transaction', 'transfer', 'payment', 'failed', 'error'],
        'fees_charges': ['fee', 'charge', 'cost', 'expensive', 'hidden'],
        'security': ['security', 'fraud', 'hack', 'safe', 'privacy', 'scam'],
        'access_availability': ['branch', 'location', 'hours', 'available', 'access'],
        'product_features': ['account', 'loan', 'card', 'feature', 'limit']
    }
    
    detected_themes = []
    for theme, keywords in themes.items():
        if any(keyword in text_lower for keyword in keywords):
            detected_themes.append(theme)
    
    return ', '.join(detected_themes) if detected_themes else 'other'

def analyze_complaint_trends(df):
    """Analyze trends in complaints over time (if date available)"""
    
    complaint_analysis = {}
    
    # Check if date column exists
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    
    if date_cols:
        date_col = date_cols[0]
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Monthly complaint trends
        monthly_complaints = df[df['sentiment_category'] == 'negative'].groupby(
            pd.Grouper(key=date_col, freq='M')
        ).size()
        
        complaint_analysis['monthly_trends'] = monthly_complaints.to_dict()
    
    # Complaint themes distribution
    negative_reviews = df[df['sentiment_category'] == 'negative'].copy()
    negative_reviews['complaint_theme'] = negative_reviews['cleaned_content'].apply(categorize_complaint_themes)
    
    theme_distribution = negative_reviews['complaint_theme'].value_counts().head(10).to_dict()
    complaint_analysis['theme_distribution'] = theme_distribution
    
    # Bank-specific complaint analysis
    bank_complaints = df[df['sentiment_category'] == 'negative'].groupby('bank_name').agg({
        'rating': 'mean',
        'sentiment_score': 'mean',
        'review_id': 'count'
    }).rename(columns={'review_id': 'complaint_count'})
    
    bank_complaints['complaint_ratio'] = bank_complaints['complaint_count'] / df.groupby('bank_name').size()
    complaint_analysis['bank_complaints'] = bank_complaints.to_dict('index')
    
    return complaint_analysis