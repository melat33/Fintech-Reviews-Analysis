"""
SENTIMENT TRENDS VISUALIZATION: Create insightful sentiment charts
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
import os

def setup_visualization():
    """Setup visualization parameters"""
    plt.style.use('seaborn-v0_8-darkgrid')
    sns.set_palette("husl")
    
    # Create output directory
    os.makedirs('../assets/plots', exist_ok=True)
    
    # Custom color palette
    colors = {
        'positive': '#2E8B57',  # Sea green
        'negative': '#DC143C',  # Crimson red
        'neutral': '#FFD700',   # Gold
        'background': '#F8F9FA'
    }
    
    return colors

def create_sentiment_distribution_chart(df, colors):
    """Create sentiment distribution chart"""
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Plot 1: Overall sentiment distribution
    sentiment_counts = df['sentiment_category'].value_counts()
    axes[0].pie(sentiment_counts.values, labels=sentiment_counts.index, 
                autopct='%1.1f%%', colors=[colors[cat] for cat in sentiment_counts.index])
    axes[0].set_title('Overall Sentiment Distribution', fontsize=14, fontweight='bold')
    
    # Plot 2: Sentiment by bank
    sentiment_by_bank = pd.crosstab(df['bank_name'], df['sentiment_category'], normalize='index')
    sentiment_by_bank.plot(kind='bar', stacked=True, ax=axes[1], 
                          color=[colors[cat] for cat in sentiment_by_bank.columns])
    axes[1].set_title('Sentiment Distribution by Bank', fontsize=14, fontweight='bold')
    axes[1].set_ylabel('Proportion')
    axes[1].set_xlabel('Bank')
    axes[1].legend(title='Sentiment')
    axes[1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig('../assets/plots/sentiment_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return '../assets/plots/sentiment_distribution.png'

def create_sentiment_trend_chart(df, colors):
    """Create sentiment trend over time (if date available)"""
    
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    
    if date_cols:
        date_col = date_cols[0]
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Monthly sentiment trend
        monthly_sentiment = df.groupby(pd.Grouper(key=date_col, freq='M')).agg({
            'sentiment_score': 'mean',
            'review_id': 'count'
        }).rename(columns={'review_id': 'review_count'})
        
        fig, axes = plt.subplots(2, 1, figsize=(12, 10))
        
        # Plot 1: Average sentiment trend
        axes[0].plot(monthly_sentiment.index, monthly_sentiment['sentiment_score'], 
                    marker='o', linewidth=2, color=colors['positive'])
        axes[0].fill_between(monthly_sentiment.index, 0, monthly_sentiment['sentiment_score'], 
                            alpha=0.3, color=colors['positive'])
        axes[0].set_title('Monthly Average Sentiment Trend', fontsize=14, fontweight='bold')
        axes[0].set_ylabel('Sentiment Score')
        axes[0].grid(True, alpha=0.3)
        
        # Plot 2: Review volume
        axes[1].bar(monthly_sentiment.index, monthly_sentiment['review_count'], 
                   color=colors['neutral'], alpha=0.7)
        axes[1].set_title('Monthly Review Volume', fontsize=14, fontweight='bold')
        axes[1].set_ylabel('Number of Reviews')
        axes[1].set_xlabel('Month')
        axes[1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig('../assets/plots/sentiment_trend.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        return '../assets/plots/sentiment_trend.png'
    
    return None

def create_sentiment_rating_correlation(df, colors):
    """Create sentiment-rating correlation visualization"""
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Plot 1: Scatter plot
    scatter = axes[0].scatter(df['rating'], df['sentiment_score'], 
                             c=df['sentiment_score'], cmap='RdYlGn', 
                             alpha=0.6, edgecolors='w', linewidth=0.5)
    axes[0].set_xlabel('Star Rating (1-5)')
    axes[0].set_ylabel('Sentiment Score (-1 to 1)')
    axes[0].set_title('Rating vs. Sentiment Correlation', fontsize=14, fontweight='bold')
    plt.colorbar(scatter, ax=axes[0])
    
    # Plot 2: Box plot by rating
    sns.boxplot(x='rating', y='sentiment_score', data=df, ax=axes[1], 
                palette='RdYlGn')
    axes[1].set_xlabel('Star Rating')
    axes[1].set_ylabel('Sentiment Score')
    axes[1].set_title('Sentiment Distribution by Rating', fontsize=14, fontweight='bold')
    
    # Calculate and display correlation
    correlation = df['rating'].corr(df['sentiment_score'])
    axes[0].text(0.05, 0.95, f'Correlation: {correlation:.3f}', 
                transform=axes[0].transAxes, fontsize=12,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='w', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig('../assets/plots/sentiment_rating_correlation.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return '../assets/plots/sentiment_rating_correlation.png'

def create_all_sentiment_visualizations(df, sentiment_df):
    """Create all sentiment visualizations"""
    
    print("\n📊 CREATING SENTIMENT VISUALIZATIONS...")
    
    colors = setup_visualization()
    
    # Create visualizations
    dist_path = create_sentiment_distribution_chart(df, colors)
    trend_path = create_sentiment_trend_chart(df, colors)
    corr_path = create_sentiment_rating_correlation(df, colors)
    
    print(f"   ✓ Created sentiment distribution chart: {dist_path}")
    if trend_path:
        print(f"   ✓ Created sentiment trend chart: {trend_path}")
    print(f"   ✓ Created sentiment-rating correlation: {corr_path}")
    
    return {
        'distribution': dist_path,
        'trend': trend_path,
        'correlation': corr_path
    }