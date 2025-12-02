"""
Simplified visualizations for Task 4
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import os

def setup_visualization():
    """Setup visualization parameters"""
    plt.style.use('seaborn-v0_8-darkgrid')
    sns.set_palette("husl")
    
    # Create output directory
    plots_dir = os.path.join('assets', 'plots')
    os.makedirs(plots_dir, exist_ok=True)
    
    # Custom color palette
    colors = {
        'positive': '#2E8B57',  # Sea green
        'negative': '#DC143C',  # Crimson red
        'neutral': '#FFD700',   # Gold
        'background': '#F8F9FA'
    }
    
    return colors, plots_dir

def create_all_visualizations(df, results):
    """Create all visualizations"""
    
    print("\n📊 CREATING VISUALIZATIONS...")
    
    colors, plots_dir = setup_visualization()
    viz_paths = []
    
    # 1. Sentiment Distribution
    try:
        if 'sentiment_category' in df.columns:
            plt.figure(figsize=(10, 6))
            sentiment_counts = df['sentiment_category'].value_counts()
            
            bars = plt.bar(sentiment_counts.index, sentiment_counts.values, 
                          color=[colors[cat] for cat in sentiment_counts.index])
            plt.title('Sentiment Distribution', fontsize=14, fontweight='bold')
            plt.xlabel('Sentiment')
            plt.ylabel('Count')
            
            # Add count labels
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height + 5,
                        f'{int(height)}', ha='center', va='bottom')
            
            sentiment_path = os.path.join(plots_dir, 'sentiment_distribution.png')
            plt.savefig(sentiment_path, dpi=300, bbox_inches='tight')
            plt.close()
            viz_paths.append(sentiment_path)
            print(f"   ✓ Created sentiment distribution: {sentiment_path}")
    except Exception as e:
        print(f"   ⚠️  Could not create sentiment chart: {e}")
    
    # 2. Rating Distribution
    try:
        plt.figure(figsize=(10, 6))
        rating_counts = df['rating'].value_counts().sort_index()
        plt.bar(rating_counts.index, rating_counts.values, 
                color=plt.cm.RdYlGn(np.linspace(0.2, 0.8, 5)))
        plt.title('Rating Distribution', fontsize=14, fontweight='bold')
        plt.xlabel('Star Rating')
        plt.ylabel('Count')
        plt.xticks(range(1, 6))
        
        rating_path = os.path.join(plots_dir, 'rating_distribution.png')
        plt.savefig(rating_path, dpi=300, bbox_inches='tight')
        plt.close()
        viz_paths.append(rating_path)
        print(f"   ✓ Created rating distribution: {rating_path}")
    except Exception as e:
        print(f"   ⚠️  Could not create rating chart: {e}")
    
    # 3. Bank Comparison Chart
    try:
        plt.figure(figsize=(12, 8))
        
        # Prepare bank data
        banks = df['bank_name'].unique()
        avg_ratings = []
        review_counts = []
        
        for bank in banks:
            bank_data = df[df['bank_name'] == bank]
            avg_ratings.append(bank_data['rating'].mean())
            review_counts.append(len(bank_data))
        
        # Normalize for bubble size
        max_size = max(review_counts)
        bubble_sizes = [size/max_size * 1000 for size in review_counts]
        
        # Create scatter plot
        scatter = plt.scatter(range(len(banks)), avg_ratings, s=bubble_sizes, 
                             alpha=0.6, c=avg_ratings, cmap='RdYlGn')
        
        plt.xticks(range(len(banks)), banks, rotation=45)
        plt.xlabel('Bank')
        plt.ylabel('Average Rating')
        plt.title('Bank Performance Comparison', fontsize=14, fontweight='bold')
        plt.colorbar(scatter, label='Average Rating')
        
        # Add labels
        for i, (bank, rating, count) in enumerate(zip(banks, avg_ratings, review_counts)):
            plt.text(i, rating + 0.05, f'{rating:.2f}', ha='center', fontsize=9)
            plt.text(i, rating - 0.15, f'({count})', ha='center', fontsize=8, alpha=0.7)
        
        bank_path = os.path.join(plots_dir, 'bank_comparison.png')
        plt.savefig(bank_path, dpi=300, bbox_inches='tight')
        plt.close()
        viz_paths.append(bank_path)
        print(f"   ✓ Created bank comparison: {bank_path}")
    except Exception as e:
        print(f"   ⚠️  Could not create bank comparison: {e}")
    
    # 4. Pain Points Chart
    try:
        if 'pain_points' in results and results['pain_points']:
            plt.figure(figsize=(12, 6))
            
            pain_names = list(results['pain_points'].keys())
            percentages = [p['percentage'] for p in results['pain_points'].values()]
            
            bars = plt.barh(pain_names, percentages, 
                           color=plt.cm.Reds(np.linspace(0.3, 0.9, len(pain_names))))
            
            plt.xlabel('Percentage of Negative Reviews (%)')
            plt.title('Top Pain Points', fontsize=14, fontweight='bold')
            
            # Add percentage labels
            for bar, percentage in zip(bars, percentages):
                width = bar.get_width()
                plt.text(width + 0.5, bar.get_y() + bar.get_height()/2,
                        f'{percentage:.1f}%', va='center', fontsize=10)
            
            pain_path = os.path.join(plots_dir, 'pain_points.png')
            plt.savefig(pain_path, dpi=300, bbox_inches='tight')
            plt.close()
            viz_paths.append(pain_path)
            print(f"   ✓ Created pain points chart: {pain_path}")
    except Exception as e:
        print(f"   ⚠️  Could not create pain points chart: {e}")
    
    print(f"   ✓ Created {len(viz_paths)} visualizations total")
    
    return viz_paths