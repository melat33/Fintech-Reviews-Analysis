"""
RATING DISTRIBUTION VISUALIZATIONS: Create rating analysis charts
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

def create_rating_distribution_chart(df):
    """Create rating distribution visualizations"""
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # Plot 1: Overall rating distribution
    rating_counts = df['rating'].value_counts().sort_index()
    axes[0, 0].bar(rating_counts.index, rating_counts.values, 
                   color=plt.cm.RdYlGn(np.linspace(0.2, 0.8, 5)))
    axes[0, 0].set_title('Overall Rating Distribution', fontsize=14, fontweight='bold')
    axes[0, 0].set_xlabel('Star Rating')
    axes[0, 0].set_ylabel('Number of Reviews')
    axes[0, 0].set_xticks(range(1, 6))
    
    # Add percentage labels
    total = rating_counts.sum()
    for i, count in enumerate(rating_counts.values):
        percentage = (count / total) * 100
        axes[0, 0].text(i + 1, count + 5, f'{percentage:.1f}%', 
                       ha='center', va='bottom', fontsize=10)
    
    # Plot 2: Rating distribution by bank
    rating_by_bank = df.groupby('bank_name')['rating'].mean().sort_values()
    axes[0, 1].barh(range(len(rating_by_bank)), rating_by_bank.values,
                   color=plt.cm.RdYlGn((rating_by_bank.values - 1) / 4))
    axes[0, 1].set_yticks(range(len(rating_by_bank)))
    axes[0, 1].set_yticklabels(rating_by_bank.index)
    axes[0, 1].set_title('Average Rating by Bank', fontsize=14, fontweight='bold')
    axes[0, 1].set_xlabel('Average Rating')
    
    # Add rating values
    for i, rating in enumerate(rating_by_bank.values):
        axes[0, 1].text(rating + 0.05, i, f'{rating:.2f}', 
                       va='center', fontsize=10)
    
    # Plot 3: Rating vs sentiment heatmap
    rating_sentiment_crosstab = pd.crosstab(df['rating'], df['sentiment_category'])
    sns.heatmap(rating_sentiment_crosstab, annot=True, fmt='d', cmap='YlOrRd',
                ax=axes[1, 0], cbar_kws={'label': 'Count'})
    axes[1, 0].set_title('Rating vs Sentiment Heatmap', fontsize=14, fontweight='bold')
    axes[1, 0].set_xlabel('Sentiment Category')
    axes[1, 0].set_ylabel('Star Rating')
    
    # Plot 4: Rating trend over time (if date available)
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    if date_cols:
        date_col = date_cols[0]
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        monthly_rating = df.groupby(pd.Grouper(key=date_col, freq='M'))['rating'].mean()
        axes[1, 1].plot(monthly_rating.index, monthly_rating.values, 
                       marker='o', linewidth=2, color='#2E8B57')
        axes[1, 1].fill_between(monthly_rating.index, 0, monthly_rating.values,
                               alpha=0.3, color='#2E8B57')
        axes[1, 1].set_title('Monthly Average Rating Trend', fontsize=14, fontweight='bold')
        axes[1, 1].set_xlabel('Month')
        axes[1, 1].set_ylabel('Average Rating')
        axes[1, 1].tick_params(axis='x', rotation=45)
        axes[1, 1].grid(True, alpha=0.3)
    else:
        # Alternative: Rating distribution density
        for rating in range(1, 6):
            rating_data = df[df['rating'] == rating]['sentiment_score']
            if len(rating_data) > 0:
                sns.kdeplot(rating_data, ax=axes[1, 1], label=f'{rating} stars', linewidth=2)
        axes[1, 1].set_title('Sentiment Distribution by Rating', fontsize=14, fontweight='bold')
        axes[1, 1].set_xlabel('Sentiment Score')
        axes[1, 1].set_ylabel('Density')
        axes[1, 1].legend()
    
    plt.tight_layout()
    plt.savefig('../assets/plots/rating_distributions.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return '../assets/plots/rating_distributions.png'

def create_bank_comparison_radar(df):
    """Create radar chart for bank comparison"""
    
    # Calculate metrics for each bank
    bank_metrics = df.groupby('bank_name').agg({
        'rating': ['mean', 'count'],
        'sentiment_score': 'mean'
    }).round(3)
    
    bank_metrics.columns = ['avg_rating', 'review_count', 'avg_sentiment']
    
    # Normalize metrics for radar chart
    metrics_normalized = pd.DataFrame()
    for col in ['avg_rating', 'review_count', 'avg_sentiment']:
        metrics_normalized[col] = (bank_metrics[col] - bank_metrics[col].min()) / \
                                 (bank_metrics[col].max() - bank_metrics[col].min())
    
    # Create radar chart
    categories = ['Avg Rating', 'Review Count', 'Avg Sentiment']
    N = len(categories)
    
    # Create angles for radar chart
    angles = [n / float(N) * 2 * np.pi for n in range(N)]
    angles += angles[:1]  # Close the loop
    
    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))
    
    # Plot each bank
    colors = plt.cm.Set3(np.linspace(0, 1, len(bank_metrics)))
    for idx, (bank, row) in enumerate(metrics_normalized.iterrows()):
        values = row.values.tolist()
        values += values[:1]  # Close the loop
        
        ax.plot(angles, values, linewidth=2, linestyle='solid', 
                label=bank, color=colors[idx])
        ax.fill(angles, values, alpha=0.1, color=colors[idx])
    
    # Add labels
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, fontsize=12)
    ax.set_ylim(0, 1)
    ax.set_title('Bank Performance Radar Chart', fontsize=16, fontweight='bold', pad=20)
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0))
    
    plt.tight_layout()
    plt.savefig('../assets/plots/bank_comparison_radar.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return '../assets/plots/bank_comparison_radar.png'

def create_all_rating_visualizations(df):
    """Create all rating visualizations"""
    
    print("\n⭐ CREATING RATING VISUALIZATIONS...")
    
    # Create output directory
    os.makedirs('../assets/plots', exist_ok=True)
    
    # Create visualizations
    dist_path = create_rating_distribution_chart(df)
    radar_path = create_bank_comparison_radar(df)
    
    print(f"   ✓ Created rating distribution chart: {dist_path}")
    print(f"   ✓ Created bank comparison radar: {radar_path}")
    
    return {
        'distribution': dist_path,
        'radar': radar_path
    }