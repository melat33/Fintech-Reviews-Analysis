"""
BANK COMPARISON ANALYSIS: Comparative metrics across all banks
"""

import pandas as pd
import numpy as np
from scipy import stats

def generate_comparative_analysis(df, sentiment_df):
    """Generate comprehensive comparative analysis across banks"""
    
    print("\n🏦 PERFORMING COMPARATIVE BANK ANALYSIS...")
    
    banks = df['bank_name'].unique()
    comparison_results = {}
    
    for bank in banks:
        bank_data = df[df['bank_name'] == bank]
        bank_sentiment = sentiment_df[sentiment_df['bank_name'] == bank] if 'bank_name' in sentiment_df.columns else sentiment_df
        
        # Key metrics
        metrics = {
            'total_reviews': len(bank_data),
            'avg_rating': bank_data['rating'].mean(),
            'avg_sentiment': bank_data['sentiment_score'].mean() if 'sentiment_score' in bank_data.columns else 0,
            'positive_ratio': (bank_data['sentiment_category'] == 'positive').mean() if 'sentiment_category' in bank_data.columns else 0,
            'negative_ratio': (bank_data['sentiment_category'] == 'negative').mean() if 'sentiment_category' in bank_data.columns else 0,
            'rating_std': bank_data['rating'].std(),
            'sentiment_std': bank_data['sentiment_score'].std() if 'sentiment_score' in bank_data.columns else 0
        }
        
        # Rating distribution
        rating_dist = bank_data['rating'].value_counts().sort_index()
        metrics['rating_distribution'] = {f"rating_{int(k)}": v for k, v in rating_dist.to_dict().items()}
        
        # Sentiment distribution
        if 'sentiment_category' in bank_data.columns:
            sentiment_dist = bank_data['sentiment_category'].value_counts()
            metrics['sentiment_distribution'] = sentiment_dist.to_dict()
        
        # Top positive keywords
        positive_reviews = bank_data[bank_data['sentiment_category'] == 'positive']
        if len(positive_reviews) > 0:
            all_words = ' '.join(positive_reviews['cleaned_content'].astype(str)).split()
            word_freq = Counter([w for w in all_words if len(w) > 3])
            metrics['top_positive_words'] = dict(word_freq.most_common(10))
        
        # Top negative keywords
        negative_reviews = bank_data[bank_data['sentiment_category'] == 'negative']
        if len(negative_reviews) > 0:
            all_words = ' '.join(negative_reviews['cleaned_content'].astype(str)).split()
            word_freq = Counter([w for w in all_words if len(w) > 3])
            metrics['top_negative_words'] = dict(word_freq.most_common(10))
        
        comparison_results[bank] = metrics
    
    # Calculate rankings
    rankings_df = pd.DataFrame(comparison_results).T
    
    # Rank by key metrics
    rankings_df['rating_rank'] = rankings_df['avg_rating'].rank(ascending=False, method='min')
    rankings_df['sentiment_rank'] = rankings_df['avg_sentiment'].rank(ascending=False, method='min')
    rankings_df['positive_ratio_rank'] = rankings_df['positive_ratio'].rank(ascending=False, method='min')
    
    # Calculate overall score (weighted average)
    rankings_df['overall_score'] = (
        rankings_df['avg_rating'] * 0.4 +
        rankings_df['avg_sentiment'] * 0.3 +
        rankings_df['positive_ratio'] * 0.3
    )
    rankings_df['overall_rank'] = rankings_df['overall_score'].rank(ascending=False, method='min')
    
    # Identify strengths and weaknesses
    for bank in banks:
        bank_row = rankings_df.loc[bank]
        
        # Strengths (metrics where bank is top 2)
        strengths = []
        if bank_row['rating_rank'] <= 2:
            strengths.append(f"High average rating ({bank_row['avg_rating']:.2f}/5)")
        if bank_row['positive_ratio_rank'] <= 2:
            strengths.append(f"High positive sentiment ratio ({bank_row['positive_ratio']:.1%})")
        
        # Weaknesses (metrics where bank is bottom 2)
        weaknesses = []
        if bank_row['rating_rank'] >= len(banks) - 1:
            weaknesses.append(f"Low average rating ({bank_row['avg_rating']:.2f}/5)")
        if bank_row['negative_ratio'] > rankings_df['negative_ratio'].median():
            weaknesses.append(f"High negative sentiment ratio ({bank_row['negative_ratio']:.1%})")
        
        comparison_results[bank]['strengths'] = strengths
        comparison_results[bank]['weaknesses'] = weaknesses
        comparison_results[bank]['overall_rank'] = int(bank_row['overall_rank'])
        comparison_results[bank]['overall_score'] = bank_row['overall_score']
    
    # Save comparison results
    comparison_df = pd.DataFrame(comparison_results).T
    comparison_df.to_csv('../data/bank_comparison_metrics.csv')
    
    print(f"   ✓ Analyzed {len(banks)} banks")
    print(f"   ✓ Generated comparative metrics")
    print(f"   ✓ Calculated rankings and identified strengths/weaknesses")
    
    return comparison_results