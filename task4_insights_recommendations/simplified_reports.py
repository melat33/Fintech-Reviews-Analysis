"""
Simplified report generation for Task 4
"""

import pandas as pd
import os
from datetime import datetime

def generate_executive_summary(df, results):
    """Generate executive summary report"""
    
    reports_dir = os.path.join('reports')
    os.makedirs(reports_dir, exist_ok=True)
    
    # Prepare summary content
    summary_content = f"""
# EXECUTIVE SUMMARY: Financial Institutions Customer Analytics
## Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
## Total Reviews Analyzed: {len(df):,}

## 📊 OVERVIEW

### Key Metrics:
- **Average Rating**: {df['rating'].mean():.2f}/5.0
- **Total Banks Analyzed**: {df['bank_name'].nunique()}
- **Date Range**: {df['review_date'].min()} to {df['review_date'].max()}

"""
    
    # Add sentiment analysis if available
    if 'sentiment_category' in df.columns:
        sentiment_counts = df['sentiment_category'].value_counts()
        total = len(df)
        
        summary_content += "### Sentiment Analysis:\n"
        for sentiment, count in sentiment_counts.items():
            percentage = (count / total) * 100
            summary_content += f"- **{sentiment.capitalize()}**: {count:,} reviews ({percentage:.1f}%)\n"
    
    # Add bank performance
    summary_content += "\n## 🏦 BANK PERFORMANCE RANKINGS\n\n"
    
    bank_stats = []
    for bank in df['bank_name'].unique():
        bank_data = df[df['bank_name'] == bank]
        stats = {
            'Bank': bank,
            'Reviews': len(bank_data),
            'Avg Rating': bank_data['rating'].mean(),
            '5-Star %': (bank_data['rating'] == 5).mean() * 100,
            '1-Star %': (bank_data['rating'] == 1).mean() * 100
        }
        
        if 'sentiment_score' in bank_data.columns:
            stats['Avg Sentiment'] = bank_data['sentiment_score'].mean()
        
        bank_stats.append(stats)
    
    # Sort by average rating
    bank_stats.sort(key=lambda x: x['Avg Rating'], reverse=True)
    
    for i, stats in enumerate(bank_stats, 1):
        summary_content += f"### {i}. {stats['Bank']}\n"
        summary_content += f"- **Average Rating**: {stats['Avg Rating']:.2f}/5.0\n"
        summary_content += f"- **Total Reviews**: {stats['Reviews']:,}\n"
        summary_content += f"- **5-Star Reviews**: {stats['5-Star %']:.1f}%\n"
        summary_content += f"- **1-Star Reviews**: {stats['1-Star %']:.1f}%\n"
        if 'Avg Sentiment' in stats:
            summary_content += f"- **Average Sentiment**: {stats['Avg Sentiment']:.3f}\n"
        summary_content += "\n"
    
    # Add insights from results
    summary_content += "## 🔍 KEY INSIGHTS\n\n"
    
    if 'top_positive_words' in results:
        summary_content += "### Top Positive Drivers:\n"
        for word, count in list(results['top_positive_words'].items())[:5]:
            summary_content += f"- **{word}**: {count} mentions\n"
        summary_content += "\n"
    
    if 'pain_points' in results:
        summary_content += "### Critical Pain Points:\n"
        for pain_point, data in results['pain_points'].items():
            summary_content += f"- **{pain_point.replace('_', ' ').title()}**: {data['count']} mentions ({data['percentage']:.1f}% of negative reviews)\n"
        summary_content += "\n"
    
    # Add recommendations
    summary_content += """## 🚀 STRATEGIC RECOMMENDATIONS

### Immediate Actions (1-3 months):
1. **Address Technical Issues**: Prioritize fixing mobile app and website problems
2. **Improve Customer Service**: Reduce response times and enhance staff training
3. **Enhance Transparency**: Clearly communicate all fees and charges

### Medium-term Initiatives (3-6 months):
1. **Digital Experience Upgrade**: Modernize online banking platforms
2. **Proactive Customer Support**: Implement AI-driven support systems
3. **Competitive Benchmarking**: Regularly compare performance with industry leaders

### Long-term Strategy (6-12 months):
1. **Personalized Banking**: Develop customized banking experiences
2. **Innovation Pipeline**: Continuously introduce new digital features
3. **Customer Loyalty Programs**: Reward long-term customers

## 📈 EXPECTED OUTCOMES

### Quantitative Targets:
- **20% reduction** in negative reviews within 6 months
- **0.3 point increase** in average rating
- **15% improvement** in customer satisfaction scores
- **25% faster** complaint resolution times

### Qualitative Benefits:
- Enhanced brand reputation
- Increased customer loyalty
- Competitive market positioning
- Data-driven decision making

## 📋 IMPLEMENTATION ROADMAP

### Phase 1: Quick Wins (Month 1-3)
- Address top 3 pain points
- Implement customer service training
- Launch transparent fee communication

### Phase 2: System Improvements (Month 4-6)
- Deploy digital banking enhancements
- Establish customer feedback system
- Create performance dashboards

### Phase 3: Innovation (Month 7-12)
- Develop personalized features
- Implement advanced analytics
- Create customer loyalty programs

---
*Report generated automatically from customer review analysis*
*Data Source: {df['source'].iloc[0] if 'source' in df.columns and len(df) > 0 else 'Customer Reviews'}*
"""
    
    # Save report
    summary_path = os.path.join(reports_dir, 'executive_summary.md')
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(summary_content)
    
    print(f"   ✓ Generated executive summary: {summary_path}")
    return summary_path

def generate_bank_reports(df):
    """Generate bank-specific reports"""
    
    bank_reports_dir = os.path.join('reports', 'bank_specific_reports')
    os.makedirs(bank_reports_dir, exist_ok=True)
    
    report_paths = []
    
    for bank in df['bank_name'].unique():
        bank_data = df[df['bank_name'] == bank]
        
        report_content = f"""
# BANK ANALYSIS REPORT: {bank}
## Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
## Total Reviews: {len(bank_data):,}

## 📊 PERFORMANCE METRICS

### Overall Statistics:
- **Average Rating**: {bank_data['rating'].mean():.2f}/5.0
- **Date Range**: {bank_data['review_date'].min()} to {bank_data['review_date'].max()}
"""
        
        # Add sentiment if available
        if 'sentiment_score' in bank_data.columns:
            report_content += f"- **Average Sentiment**: {bank_data['sentiment_score'].mean():.3f}\n"
        
        if 'sentiment_category' in bank_data.columns:
            positive_pct = (bank_data['sentiment_category'] == 'positive').mean() * 100
            negative_pct = (bank_data['sentiment_category'] == 'negative').mean() * 100
            report_content += f"- **Positive Reviews**: {positive_pct:.1f}%\n"
            report_content += f"- **Negative Reviews**: {negative_pct:.1f}%\n"
        
        # Rating distribution
        report_content += "\n### Rating Distribution:\n"
        rating_dist = bank_data['rating'].value_counts().sort_index()
        for rating, count in rating_dist.items():
            percentage = (count / len(bank_data)) * 100
            stars = "⭐" * int(rating)
            report_content += f"- **{rating} Stars** {stars}: {count:,} reviews ({percentage:.1f}%)\n"
        
        # Sample reviews
        report_content += "\n## 💬 CUSTOMER FEEDBACK SUMMARY\n\n"
        
        # Top positive reviews
        positive_reviews = bank_data[bank_data['rating'] >= 4].head(3)
        if len(positive_reviews) > 0:
            report_content += "### What Customers Like:\n"
            for _, review in positive_reviews.iterrows():
                snippet = review['review_text'][:150] + "..." if len(review['review_text']) > 150 else review['review_text']
                report_content += f"- \"{snippet}\"\n"
        
        # Top negative reviews
        negative_reviews = bank_data[bank_data['rating'] <= 2].head(3)
        if len(negative_reviews) > 0:
            report_content += "\n### Areas for Improvement:\n"
            for _, review in negative_reviews.iterrows():
                snippet = review['review_text'][:150] + "..." if len(review['review_text']) > 150 else review['review_text']
                report_content += f"- \"{snippet}\"\n"
        
        # Recommendations
        report_content += f"""
## 🎯 RECOMMENDATIONS FOR {bank.upper()}

### Priority Actions:
1. **Analyze 1-2 star reviews** to identify specific issues
2. **Respond to negative reviews** with solutions and follow-up
3. **Amplify positive feedback** in marketing materials
4. **Monitor sentiment trends** monthly

### Key Success Metrics:
- Increase average rating to {bank_data['rating'].mean() + 0.3:.2f}/5.0 within 6 months
- Reduce 1-2 star reviews by 20%
- Improve response rate to customer feedback

---
*This report is based on analysis of {len(bank_data):,} customer reviews*
*Generated automatically from customer feedback data*
"""
        
        # Save bank report
        bank_filename = bank.lower().replace(' ', '_').replace('(', '').replace(')', '') + '_report.md'
        bank_path = os.path.join(bank_reports_dir, bank_filename)
        
        with open(bank_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        report_paths.append(bank_path)
    
    print(f"   ✓ Generated {len(report_paths)} bank-specific reports")
    return bank_reports_dir