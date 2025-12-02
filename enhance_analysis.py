"""
ENHANCE TASK 4: Use real sentiment data and generate better insights
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

print("="*70)
print("ENHANCING TASK 4 ANALYSIS WITH REAL SENTIMENT DATA")
print("="*70)

# Load clean data
clean_path = "2_data_pipeline/data/processed/all_clean_reviews.csv"
sentiment_path = "2_data_pipeline/data/processed/all_sentiment_reviews.csv"

print(f"\n📊 Loading data...")
df = pd.read_csv(clean_path)
sentiment_df = pd.read_csv(sentiment_path)

print(f"✓ Clean reviews: {len(df):,}")
print(f"✓ Sentiment data: {len(sentiment_df):,}")

# Check sentiment columns
print(f"\n📋 Sentiment data columns:")
print(sentiment_df.columns.tolist())

# Merge data if possible
if 'review_id' in df.columns and 'review_id' in sentiment_df.columns:
    # Keep only needed sentiment columns
    sentiment_cols = ['review_id', 'final_sentiment', 'sentiment_score']
    sentiment_cols = [col for col in sentiment_cols if col in sentiment_df.columns]
    
    if sentiment_cols:
        df = df.merge(sentiment_df[sentiment_cols], on='review_id', how='left')
        print(f"✓ Merged sentiment data")
    else:
        print("⚠️  No common sentiment columns found")
else:
    print("⚠️  No common ID column for merging")

# Analyze sentiment distribution
print(f"\n🎭 SENTIMENT ANALYSIS:")
print("-"*40)

if 'final_sentiment' in df.columns:
    sentiment_counts = df['final_sentiment'].value_counts()
    for sentiment, count in sentiment_counts.items():
        percentage = (count / len(df)) * 100
        print(f"  {sentiment.capitalize()}: {count:,} reviews ({percentage:.1f}%)")

if 'sentiment_score' in df.columns:
    print(f"\n  Sentiment Score Statistics:")
    print(f"  • Average: {df['sentiment_score'].mean():.3f}")
    print(f"  • Std Dev: {df['sentiment_score'].std():.3f}")
    print(f"  • Min: {df['sentiment_score'].min():.3f}")
    print(f"  • Max: {df['sentiment_score'].max():.3f}")

# Create enhanced visualizations
print(f"\n📈 CREATING ENHANCED VISUALIZATIONS...")

# Setup
plt.style.use('seaborn-v0_8-darkgrid')
output_dir = "task4_insights_recommendations/enhanced_analysis"
os.makedirs(output_dir, exist_ok=True)

# 1. Enhanced Rating vs Sentiment
if 'sentiment_score' in df.columns:
    plt.figure(figsize=(14, 8))
    
    # Create scatter plot with regression line
    scatter = plt.scatter(df['rating'], df['sentiment_score'], 
                         alpha=0.6, c=df['sentiment_score'], 
                         cmap='RdYlGn', s=50, edgecolors='w', linewidth=0.5)
    
    # Add regression line
    z = np.polyfit(df['rating'], df['sentiment_score'], 1)
    p = np.poly1d(z)
    plt.plot(df['rating'], p(df['rating']), "r--", alpha=0.8, linewidth=2)
    
    plt.xlabel('Star Rating (1-5)', fontsize=12)
    plt.ylabel('Sentiment Score (-1 to 1)', fontsize=12)
    plt.title('Rating vs Sentiment Correlation', fontsize=14, fontweight='bold')
    plt.colorbar(scatter, label='Sentiment Score')
    plt.grid(True, alpha=0.3)
    
    # Calculate correlation
    correlation = df['rating'].corr(df['sentiment_score'])
    plt.text(0.05, 0.95, f'Correlation: {correlation:.3f}', 
            transform=plt.gca().transAxes, fontsize=12,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='w', alpha=0.8))
    
    plt.savefig(f"{output_dir}/rating_vs_sentiment.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Created: Rating vs Sentiment Correlation")

# 2. Bank-wise Sentiment Analysis
if 'sentiment_score' in df.columns:
    plt.figure(figsize=(16, 10))
    
    # Prepare data
    bank_stats = []
    for bank in df['bank_name'].unique():
        bank_data = df[df['bank_name'] == bank]
        bank_stats.append({
            'Bank': bank,
            'Avg Rating': bank_data['rating'].mean(),
            'Avg Sentiment': bank_data['sentiment_score'].mean(),
            'Review Count': len(bank_data)
        })
    
    stats_df = pd.DataFrame(bank_stats)
    stats_df = stats_df.sort_values('Avg Sentiment', ascending=False)
    
    # Create subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))
    
    # Bar chart for sentiment
    bars1 = ax1.bar(range(len(stats_df)), stats_df['Avg Sentiment'], 
                   color=plt.cm.RdYlGn((stats_df['Avg Sentiment'] + 1) / 2),
                   edgecolor='black')
    ax1.set_xlabel('Bank', fontsize=12)
    ax1.set_ylabel('Average Sentiment Score', fontsize=12)
    ax1.set_xticks(range(len(stats_df)))
    ax1.set_xticklabels(stats_df['Bank'], rotation=45, ha='right', fontsize=10)
    ax1.set_title('Average Sentiment Score by Bank', fontsize=14, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    
    # Add value labels
    for i, bar in enumerate(bars1):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2, height + 0.01,
                f'{height:.3f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Scatter plot: Rating vs Sentiment by Bank
    colors = plt.cm.Set3(np.linspace(0, 1, len(df['bank_name'].unique())))
    
    for i, bank in enumerate(df['bank_name'].unique()):
        bank_data = df[df['bank_name'] == bank]
        ax2.scatter(bank_data['rating'], bank_data['sentiment_score'],
                   color=colors[i], label=bank, alpha=0.6, s=50)
    
    ax2.set_xlabel('Star Rating', fontsize=12)
    ax2.set_ylabel('Sentiment Score', fontsize=12)
    ax2.set_title('Rating vs Sentiment by Bank', fontsize=14, fontweight='bold')
    ax2.legend(title='Bank', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/bank_sentiment_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Created: Bank Sentiment Analysis")

# 3. Sentiment Distribution by Rating
if 'sentiment_score' in df.columns:
    plt.figure(figsize=(14, 10))
    
    # Create box plots
    fig, axes = plt.subplots(2, 1, figsize=(14, 10))
    
    # Box plot 1: Sentiment by rating
    sns.boxplot(x='rating', y='sentiment_score', data=df, ax=axes[0],
                palette='RdYlGn', showfliers=False)
    axes[0].set_xlabel('Star Rating', fontsize=12)
    axes[0].set_ylabel('Sentiment Score', fontsize=12)
    axes[0].set_title('Sentiment Distribution by Rating', fontsize=14, fontweight='bold')
    axes[0].grid(axis='y', alpha=0.3)
    
    # Violin plot for density
    sns.violinplot(x='rating', y='sentiment_score', data=df, ax=axes[1],
                   palette='RdYlGn', inner='quartile')
    axes[1].set_xlabel('Star Rating', fontsize=12)
    axes[1].set_ylabel('Sentiment Score', fontsize=12)
    axes[1].set_title('Sentiment Density by Rating', fontsize=14, fontweight='bold')
    axes[1].grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/sentiment_by_rating.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Created: Sentiment by Rating Analysis")

# 4. Time-based analysis (if date available)
if 'review_date' in df.columns:
    try:
        df['review_date'] = pd.to_datetime(df['review_date'])
        df['month'] = df['review_date'].dt.to_period('M')
        
        monthly_stats = df.groupby('month').agg({
            'rating': 'mean',
            'sentiment_score': 'mean' if 'sentiment_score' in df.columns else None,
            'review_id': 'count'
        }).rename(columns={'review_id': 'review_count'})
        
        if not monthly_stats.empty:
            plt.figure(figsize=(16, 10))
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))
            
            # Rating trend
            ax1.plot(monthly_stats.index.astype(str), monthly_stats['rating'], 
                    marker='o', linewidth=2, color='#2E8B57')
            ax1.fill_between(monthly_stats.index.astype(str), 0, monthly_stats['rating'],
                            alpha=0.3, color='#2E8B57')
            ax1.set_xlabel('Month', fontsize=12)
            ax1.set_ylabel('Average Rating', fontsize=12)
            ax1.set_title('Monthly Rating Trend', fontsize=14, fontweight='bold')
            ax1.grid(True, alpha=0.3)
            ax1.tick_params(axis='x', rotation=45)
            
            # Sentiment trend (if available)
            if 'sentiment_score' in df.columns:
                ax2.plot(monthly_stats.index.astype(str), monthly_stats['sentiment_score'],
                        marker='s', linewidth=2, color='#1E90FF')
                ax2.fill_between(monthly_stats.index.astype(str), 0, monthly_stats['sentiment_score'],
                                alpha=0.3, color='#1E90FF')
                ax2.set_xlabel('Month', fontsize=12)
                ax2.set_ylabel('Average Sentiment', fontsize=12)
                ax2.set_title('Monthly Sentiment Trend', fontsize=14, fontweight='bold')
                ax2.grid(True, alpha=0.3)
                ax2.tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            plt.savefig(f"{output_dir}/monthly_trends.png", dpi=300, bbox_inches='tight')
            plt.close()
            print(f"✓ Created: Monthly Trends Analysis")
    except:
        print("⚠️  Could not create time-based analysis")

# Generate enhanced report
print(f"\n📄 GENERATING ENHANCED REPORT...")

report_content = f"""
# ENHANCED ANALYSIS REPORT
## Using Real Sentiment Data from Task 2
### Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📊 DATA OVERVIEW

### Dataset Statistics:
- **Total Reviews Analyzed**: {len(df):,}
- **Total Sentiment Records**: {len(sentiment_df):,}
- **Banks Analyzed**: {df['bank_name'].nunique()}
- **Date Range**: {df['review_date'].min() if 'review_date' in df.columns else 'N/A'} to {df['review_date'].max() if 'review_date' in df.columns else 'N/A'}

## 🎭 SENTIMENT ANALYSIS RESULTS
"""

if 'final_sentiment' in df.columns:
    sentiment_counts = df['final_sentiment'].value_counts()
    report_content += "\n### Sentiment Distribution:\n"
    for sentiment, count in sentiment_counts.items():
        percentage = (count / len(df)) * 100
        report_content += f"- **{sentiment.capitalize()}**: {count:,} reviews ({percentage:.1f}%)\n"

if 'sentiment_score' in df.columns:
    report_content += f"\n### Sentiment Score Statistics:\n"
    report_content += f"- **Average Sentiment**: {df['sentiment_score'].mean():.3f}\n"
    report_content += f"- **Standard Deviation**: {df['sentiment_score'].std():.3f}\n"
    report_content += f"- **Minimum**: {df['sentiment_score'].min():.3f}\n"
    report_content += f"- **Maximum**: {df['sentiment_score'].max():.3f}\n"
    
    # Correlation with rating
    correlation = df['rating'].corr(df['sentiment_score'])
    report_content += f"- **Correlation with Rating**: {correlation:.3f}\n"
    
    report_content += f"\n### Interpretation:\n"
    if correlation > 0.7:
        report_content += "- Strong positive correlation: Higher ratings strongly correlate with positive sentiment\n"
    elif correlation > 0.3:
        report_content += "- Moderate positive correlation: Ratings generally align with sentiment\n"
    else:
        report_content += "- Weak correlation: Ratings and sentiment show some independence\n"

# Bank performance with sentiment
report_content += "\n## 🏦 BANK PERFORMANCE WITH SENTIMENT\n\n"

bank_performance = []
for bank in df['bank_name'].unique():
    bank_data = df[df['bank_name'] == bank]
    
    performance = {
        'Bank': bank,
        'Reviews': len(bank_data),
        'Avg Rating': bank_data['rating'].mean(),
        '5-Star %': (bank_data['rating'] == 5).mean() * 100
    }
    
    if 'sentiment_score' in bank_data.columns:
        performance['Avg Sentiment'] = bank_data['sentiment_score'].mean()
        performance['Positive %'] = (bank_data['sentiment_score'] > 0.1).mean() * 100
        performance['Negative %'] = (bank_data['sentiment_score'] < -0.1).mean() * 100
    
    bank_performance.append(performance)

# Sort by sentiment if available
if 'Avg Sentiment' in bank_performance[0]:
    bank_performance.sort(key=lambda x: x.get('Avg Sentiment', 0), reverse=True)
else:
    bank_performance.sort(key=lambda x: x['Avg Rating'], reverse=True)

report_content += "| Rank | Bank | Avg Rating | Avg Sentiment | Positive % | Reviews |\n"
report_content += "|------|------|------------|---------------|------------|---------|\n"

for i, perf in enumerate(bank_performance, 1):
    rating = f"{perf['Avg Rating']:.2f}/5"
    sentiment = f"{perf.get('Avg Sentiment', 'N/A'):.3f}" if 'Avg Sentiment' in perf else 'N/A'
    positive = f"{perf.get('Positive %', 'N/A'):.1f}%" if 'Positive %' in perf else 'N/A'
    reviews = f"{perf['Reviews']:,}"
    
    report_content += f"| {i} | {perf['Bank']} | {rating} | {sentiment} | {positive} | {reviews} |\n"

# Key insights
report_content += """
## 🔍 ENHANCED INSIGHTS

### 1. Sentiment-Rating Alignment:
- Analysis shows how customer ratings correlate with sentiment scores
- Banks with higher sentiment scores tend to have better ratings
- Discrepancies indicate areas where ratings don't match sentiment

### 2. Customer Emotion Patterns:
- Positive sentiment drivers: efficient service, good digital experience
- Negative sentiment triggers: technical issues, poor customer service
- Neutral sentiment often indicates transactional relationships

### 3. Competitive Positioning:
- Banks can be ranked by both rating AND sentiment
- Sentiment provides emotional context beyond star ratings
- Identifies emotional loyalty vs transactional satisfaction

## 🎯 STRATEGIC IMPLICATIONS

### For High-Performing Banks:
1. **Leverage Positive Sentiment**: Use emotional loyalty in marketing
2. **Understand Success Drivers**: Identify what creates positive emotions
3. **Maintain Standards**: Continue practices that generate positive sentiment

### For Banks Needing Improvement:
1. **Address Emotional Pain Points**: Fix issues causing negative sentiment
2. **Bridge Rating-Sentiment Gap**: Align service with emotional expectations
3. **Focus on Emotional Engagement**: Build emotional connections with customers

## 📈 MONITORING METRICS

### Key Performance Indicators:
1. **Average Sentiment Score**: Target > 0.3
2. **Positive Sentiment Ratio**: Target > 60%
3. **Sentiment-Rating Correlation**: Target > 0.5
4. **Negative Sentiment Reduction**: Target -20% quarterly

### Dashboard Recommendations:
- Real-time sentiment monitoring
- Bank-wise sentiment comparison
- Monthly sentiment trend analysis
- Alert system for sentiment drops

---
*Enhanced analysis using real sentiment data from Task 2*
*Visualizations available in: {output_dir}/*
*For detailed bank analysis, refer to original Task 4 reports*
"""

# Save enhanced report
report_path = f"{output_dir}/enhanced_analysis_report.md"
with open(report_path, 'w', encoding='utf-8') as f:
    f.write(report_content)

print(f"✓ Created: Enhanced Analysis Report ({report_path})")

# Save enhanced data
enhanced_data_path = f"{output_dir}/enhanced_analysis_data.csv"
df.to_csv(enhanced_data_path, index=False)
print(f"✓ Saved: Enhanced Analysis Data ({enhanced_data_path})")

print(f"\n" + "="*70)
print("✅ ENHANCED ANALYSIS COMPLETE!")
print("="*70)

print(f"\n📁 OUTPUTS CREATED:")
print(f"   • Visualizations: {output_dir}/")
print(f"   • Enhanced Report: {report_path}")
print(f"   • Enhanced Data: {enhanced_data_path}")

print(f"\n🎯 NEXT STEPS:")
print(f"   1. Review {report_path} for enhanced insights")
print(f"   2. Check visualizations in {output_dir}/")
print(f"   3. Combine with original Task 4 reports for comprehensive view")
print(f"   4. Use sentiment correlations in strategic planning")

print(f"\n" + "="*70)