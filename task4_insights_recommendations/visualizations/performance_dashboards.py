"""
PERFORMANCE DASHBOARDS: Create comprehensive dashboard
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import os

def create_interactive_sentiment_dashboard(df):
    """Create interactive sentiment dashboard"""
    
    # Prepare data
    sentiment_by_bank = pd.crosstab(df['bank_name'], df['sentiment_category'], normalize='index') * 100
    
    # Create figure
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Sentiment Distribution by Bank', 'Average Rating by Bank',
                       'Review Volume by Bank', 'Sentiment-Rating Correlation'),
        specs=[[{'type': 'bar'}, {'type': 'bar'}],
               [{'type': 'bar'}, {'type': 'scatter'}]]
    )
    
    # Plot 1: Sentiment distribution by bank (stacked bar)
    for sentiment in ['positive', 'neutral', 'negative']:
        if sentiment in sentiment_by_bank.columns:
            fig.add_trace(
                go.Bar(
                    name=sentiment.capitalize(),
                    x=sentiment_by_bank.index,
                    y=sentiment_by_bank[sentiment],
                    marker_color={'positive': '#2E8B57', 'neutral': '#FFD700', 'negative': '#DC143C'}[sentiment]
                ),
                row=1, col=1
            )
    
    # Plot 2: Average rating by bank
    avg_rating = df.groupby('bank_name')['rating'].mean().sort_values()
    fig.add_trace(
        go.Bar(
            x=avg_rating.values,
            y=avg_rating.index,
            orientation='h',
            marker_color=px.colors.sequential.Greens,
            text=[f'{x:.2f}' for x in avg_rating.values],
            textposition='auto'
        ),
        row=1, col=2
    )
    
    # Plot 3: Review volume by bank
    review_counts = df['bank_name'].value_counts()
    fig.add_trace(
        go.Bar(
            x=review_counts.index,
            y=review_counts.values,
            marker_color=px.colors.qualitative.Set3,
            text=review_counts.values,
            textposition='auto'
        ),
        row=2, col=1
    )
    
    # Plot 4: Sentiment-rating correlation
    fig.add_trace(
        go.Scatter(
            x=df['rating'],
            y=df['sentiment_score'],
            mode='markers',
            marker=dict(
                size=8,
                color=df['sentiment_score'],
                colorscale='RdYlGn',
                showscale=True,
                colorbar=dict(title="Sentiment")
            ),
            text=df['bank_name'],
            hoverinfo='text+x+y'
        ),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        title_text="Financial Institutions Customer Analytics Dashboard",
        title_font_size=24,
        title_x=0.5,
        showlegend=True,
        height=900,
        template="plotly_white"
    )
    
    # Update axes
    fig.update_xaxes(title_text="Bank", row=1, col=1)
    fig.update_yaxes(title_text="Percentage (%)", row=1, col=1)
    fig.update_xaxes(title_text="Average Rating", row=1, col=2)
    fig.update_xaxes(title_text="Bank", row=2, col=1)
    fig.update_yaxes(title_text="Number of Reviews", row=2, col=1)
    fig.update_xaxes(title_text="Star Rating", row=2, col=2)
    fig.update_yaxes(title_text="Sentiment Score", row=2, col=2)
    
    # Save as HTML
    output_path = '../assets/dashboard_interactive.html'
    fig.write_html(output_path)
    
    return output_path

def create_performance_metrics_dashboard(comparison_results):
    """Create performance metrics dashboard"""
    
    # Convert comparison results to DataFrame
    metrics_df = pd.DataFrame(comparison_results).T
    
    # Create radar chart for each bank
    fig = go.Figure()
    
    metrics = ['avg_rating', 'positive_ratio', 'overall_score']
    metric_names = ['Average Rating', 'Positive Ratio', 'Overall Score']
    
    for bank in metrics_df.index:
        values = [metrics_df.loc[bank, metric] for metric in metrics]
        # Normalize values for radar chart
        normalized_values = []
        for i, metric in enumerate(metrics):
            min_val = metrics_df[metric].min()
            max_val = metrics_df[metric].max()
            norm_val = (values[i] - min_val) / (max_val - min_val) if max_val > min_val else 0.5
            normalized_values.append(norm_val * 100)
        
        fig.add_trace(go.Scatterpolar(
            r=normalized_values + [normalized_values[0]],  # Close the loop
            theta=metric_names + [metric_names[0]],
            fill='toself',
            name=bank
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )
        ),
        title="Bank Performance Comparison",
        showlegend=True,
        height=600
    )
    
    output_path = '../assets/performance_radar.html'
    fig.write_html(output_path)
    
    return output_path

def create_recommendation_priority_matrix(recommendations_df):
    """Create recommendation priority matrix"""
    
    if recommendations_df.empty:
        return None
    
    # Create bubble chart: Effort vs Impact
    fig = go.Figure()
    
    # Map priority to colors
    priority_colors = {
        'High': '#DC143C',  # Red
        'Medium': '#FFD700',  # Gold
        'Low': '#2E8B57'  # Green
    }
    
    # Map effort to sizes
    effort_sizes = {
        'Low': 20,
        'Low-Medium': 30,
        'Medium': 40,
        'Medium-High': 50,
        'High': 60
    }
    
    for priority in recommendations_df['priority'].unique():
        subset = recommendations_df[recommendations_df['priority'] == priority]
        
        fig.add_trace(go.Scatter(
            x=subset['roi_score'],
            y=subset['priority'].map({'High': 3, 'Medium': 2, 'Low': 1}),
            mode='markers+text',
            marker=dict(
                size=subset['implementation_effort'].map(effort_sizes),
                color=priority_colors[priority],
                opacity=0.7,
                line=dict(width=2, color='DarkSlateGrey')
            ),
            text=subset['specific_recommendation'].str[:50] + '...',
            textposition='top center',
            name=f'{priority} Priority',
            hovertext=subset.apply(lambda row: f"Bank: {row['bank']}<br>Impact: {row['expected_impact']}<br>Effort: {row['implementation_effort']}", axis=1),
            hoverinfo='text'
        ))
    
    fig.update_layout(
        title="Recommendation Priority Matrix (ROI Score vs Priority)",
        xaxis_title="ROI Score (Higher is Better)",
        yaxis_title="Priority Level",
        yaxis=dict(
            tickmode='array',
            tickvals=[1, 2, 3],
            ticktext=['Low', 'Medium', 'High']
        ),
        height=700,
        showlegend=True
    )
    
    output_path = '../assets/recommendation_matrix.html'
    fig.write_html(output_path)
    
    return output_path

def create_comprehensive_dashboard(df, sentiment_df, driver_results, comparison_results):
    """Create comprehensive dashboard with all insights"""
    
    print("\n📈 CREATING COMPREHENSIVE DASHBOARD...")
    
    # Create output directory
    os.makedirs('../assets', exist_ok=True)
    
    # Create dashboards
    sentiment_dashboard = create_interactive_sentiment_dashboard(df)
    performance_dashboard = create_performance_metrics_dashboard(comparison_results)
    
    # Load recommendations if available
    recommendations_path = '../data/recommendation_matrix.csv'
    if os.path.exists(recommendations_path):
        recommendations_df = pd.read_csv(recommendations_path)
        recommendation_dashboard = create_recommendation_priority_matrix(recommendations_df)
    else:
        recommendation_dashboard = None
    
    print(f"   ✓ Created interactive sentiment dashboard: {sentiment_dashboard}")
    print(f"   ✓ Created performance dashboard: {performance_dashboard}")
    if recommendation_dashboard:
        print(f"   ✓ Created recommendation priority matrix: {recommendation_dashboard}")
    
    # Create summary dashboard
    fig = make_subplots(
        rows=3, cols=3,
        subplot_titles=(
            'Key Metrics Summary', 'Top Pain Points', 'Bank Rankings',
            'Sentiment Distribution', 'Rating Distribution', 'Word Cloud',
            'Recommendation Timeline', 'ROI Analysis', 'Success Metrics'
        ),
        specs=[
            [{'type': 'indicator'}, {'type': 'bar'}, {'type': 'table'}],
            [{'type': 'pie'}, {'type': 'bar'}, {'type': 'image'}],
            [{'type': 'gantt'}, {'type': 'bar'}, {'type': 'indicator'}]
        ]
    )
    
    # Add summary metrics
    total_reviews = len(df)
    avg_rating = df['rating'].mean()
    avg_sentiment = df['sentiment_score'].mean()
    
    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=total_reviews,
            title={"text": "Total Reviews"},
            domain={'row': 0, 'column': 0}
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=avg_rating,
            title={"text": "Avg Rating"},
            domain={'row': 0, 'column': 0}
        ),
        row=1, col=1
    )
    
    fig.update_layout(
        title_text="Executive Summary Dashboard",
        height=1000,
        showlegend=False
    )
    
    summary_dashboard = '../assets/executive_summary.html'
    fig.write_html(summary_dashboard)
    
    print(f"   ✓ Created executive summary dashboard: {summary_dashboard}")
    
    return {
        'sentiment_dashboard': sentiment_dashboard,
        'performance_dashboard': performance_dashboard,
        'recommendation_dashboard': recommendation_dashboard,
        'executive_summary': summary_dashboard
    }