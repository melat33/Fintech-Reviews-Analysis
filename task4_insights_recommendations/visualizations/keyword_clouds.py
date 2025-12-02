"""
KEYWORD CLOUD VISUALIZATIONS: Create word clouds for insights
"""

import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
from collections import Counter
import os

def create_sentiment_wordclouds(df):
    """Create word clouds for positive and negative sentiments"""
    
    # Prepare stopwords
    stopwords = set(STOPWORDS)
    custom_stopwords = {'bank', 'service', 'customer', 'like', 'one', 'get', 'would', 'also'}
    stopwords.update(custom_stopwords)
    
    # Split data by sentiment
    positive_reviews = df[df['sentiment_category'] == 'positive']['cleaned_content']
    negative_reviews = df[df['sentiment_category'] == 'negative']['cleaned_content']
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 8))
    
    # Positive word cloud
    if len(positive_reviews) > 0:
        positive_text = ' '.join(positive_reviews.dropna().astype(str))
        positive_wc = WordCloud(
            width=800, height=400,
            background_color='white',
            stopwords=stopwords,
            colormap='Greens',
            max_words=100
        ).generate(positive_text)
        
        axes[0].imshow(positive_wc, interpolation='bilinear')
        axes[0].set_title('Positive Review Keywords', fontsize=16, fontweight='bold')
        axes[0].axis('off')
    
    # Negative word cloud
    if len(negative_reviews) > 0:
        negative_text = ' '.join(negative_reviews.dropna().astype(str))
        negative_wc = WordCloud(
            width=800, height=400,
            background_color='white',
            stopwords=stopwords,
            colormap='Reds',
            max_words=100
        ).generate(negative_text)
        
        axes[1].imshow(negative_wc, interpolation='bilinear')
        axes[1].set_title('Negative Review Keywords', fontsize=16, fontweight='bold')
        axes[1].axis('off')
    
    plt.tight_layout()
    plt.savefig('../assets/plots/sentiment_wordclouds.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return '../assets/plots/sentiment_wordclouds.png'

def create_bank_specific_wordclouds(df):
    """Create word clouds for each bank"""
    
    banks = df['bank_name'].unique()
    n_banks = len(banks)
    
    # Calculate grid dimensions
    n_cols = 3
    n_rows = (n_banks + n_cols - 1) // n_cols
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(18, 5 * n_rows))
    axes = axes.flatten()
    
    # Prepare stopwords
    stopwords = set(STOPWORDS)
    custom_stopwords = {'bank', 'service', 'customer'}
    stopwords.update(custom_stopwords)
    
    for idx, bank in enumerate(banks):
        if idx < len(axes):
            bank_reviews = df[df['bank_name'] == bank]['cleaned_content']
            
            if len(bank_reviews) > 0:
                bank_text = ' '.join(bank_reviews.dropna().astype(str))
                
                # Calculate sentiment for colormap
                bank_sentiment = df[df['bank_name'] == bank]['sentiment_score'].mean()
                colormap = 'RdYlGn'  # Red-Yellow-Green colormap
                
                wc = WordCloud(
                    width=400, height=300,
                    background_color='white',
                    stopwords=stopwords,
                    colormap=colormap,
                    max_words=50
                ).generate(bank_text)
                
                axes[idx].imshow(wc, interpolation='bilinear')
                axes[idx].set_title(f'{bank}\nAvg Sentiment: {bank_sentiment:.3f}', 
                                   fontsize=12, fontweight='bold')
                axes[idx].axis('off')
    
    # Hide empty subplots
    for idx in range(n_banks, len(axes)):
        axes[idx].axis('off')
    
    plt.tight_layout()
    plt.savefig('../assets/plots/bank_wordclouds.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return '../assets/plots/bank_wordclouds.png'

def create_pain_point_visualization(df):
    """Create visualization of pain points"""
    
    # Extract negative reviews
    negative_reviews = df[df['sentiment_category'] == 'negative']
    
    if len(negative_reviews) == 0:
        return None
    
    # Common pain point keywords
    pain_keywords = {
        'slow': ['slow', 'wait', 'delay', 'long', 'hours'],
        'fees': ['fee', 'charge', 'cost', 'expensive', 'money'],
        'technical': ['app', 'online', 'mobile', 'website', 'error'],
        'staff': ['staff', 'rude', 'unhelpful', 'attitude', 'employee'],
        'security': ['security', 'fraud', 'safe', 'privacy', 'hack']
    }
    
    # Count pain point occurrences
    pain_counts = {}
    for pain_name, keywords in pain_keywords.items():
        pattern = '|'.join(keywords)
        count = negative_reviews['cleaned_content'].str.contains(pattern, case=False, na=False).sum()
        pain_counts[pain_name] = count
    
    # Create bar chart
    fig, ax = plt.subplots(figsize=(12, 6))
    
    pain_names = list(pain_counts.keys())
    counts = list(pain_counts.values())
    
    bars = ax.bar(pain_names, counts, color=plt.cm.Reds(np.linspace(0.3, 0.9, len(pain_names))))
    
    # Add count labels
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                f'{count}', ha='center', va='bottom', fontsize=10)
    
    ax.set_title('Common Pain Points in Negative Reviews', fontsize=14, fontweight='bold')
    ax.set_ylabel('Number of Mentions')
    ax.set_xlabel('Pain Point Category')
    ax.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig('../assets/plots/pain_points_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return '../assets/plots/pain_points_analysis.png'

def create_all_wordclouds(df, sentiment_df):
    """Create all word cloud visualizations"""
    
    print("\n☁️ CREATING WORD CLOUD VISUALIZATIONS...")
    
    # Create output directory
    os.makedirs('../assets/plots', exist_ok=True)
    
    # Create visualizations
    sentiment_wc_path = create_sentiment_wordclouds(df)
    bank_wc_path = create_bank_specific_wordclouds(df)
    pain_points_path = create_pain_point_visualization(df)
    
    print(f"   ✓ Created sentiment word clouds: {sentiment_wc_path}")
    print(f"   ✓ Created bank-specific word clouds: {bank_wc_path}")
    if pain_points_path:
        print(f"   ✓ Created pain points analysis: {pain_points_path}")
    
    return {
        'sentiment_wordclouds': sentiment_wc_path,
        'bank_wordclouds': bank_wc_path,
        'pain_points': pain_points_path
    }