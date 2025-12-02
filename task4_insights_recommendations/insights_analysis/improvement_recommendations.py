"""
STRATEGIC RECOMMENDATIONS: Data-driven improvement suggestions
"""

import pandas as pd
import numpy as np

def generate_recommendations_from_pain_points(pain_points, bank_name=None):
    """Generate specific recommendations from identified pain points"""
    
    recommendations = []
    
    pain_point_mapping = {
        'slow_service': {
            'issue': 'Slow customer service and long wait times',
            'recommendations': [
                'Implement digital queue management system',
                'Increase staff during peak hours',
                'Introduce appointment scheduling for complex transactions',
                'Automate routine inquiries with AI chatbot'
            ],
            'priority': 'High',
            'expected_impact': '30% reduction in wait time complaints',
            'effort': 'Medium'
        },
        'fees_charges': {
            'issue': 'High or unexpected fees and charges',
            'recommendations': [
                'Introduce transparent fee structure with no hidden charges',
                'Create fee calculator on website/app',
                'Offer fee waivers for loyal customers',
                'Implement fee alerts before transactions'
            ],
            'priority': 'High',
            'expected_impact': '25% reduction in fee-related complaints',
            'effort': 'Low-Medium'
        },
        'technical_issues': {
            'issue': 'Mobile app and online banking technical problems',
            'recommendations': [
                'Conduct comprehensive app usability testing',
                'Establish 24/7 technical support team',
                'Implement rapid bug fix deployment process',
                'Create user feedback loop for feature improvements'
            ],
            'priority': 'High',
            'expected_impact': '40% improvement in app store ratings',
            'effort': 'High'
        },
        'staff_behavior': {
            'issue': 'Rude or unhelpful staff behavior',
            'recommendations': [
                'Implement mandatory customer service training',
                'Establish mystery shopper program',
                'Create customer service certification for staff',
                'Introduce customer feedback-based staff incentives'
            ],
            'priority': 'Medium',
            'expected_impact': '50% reduction in staff-related complaints',
            'effort': 'Medium'
        }
    }
    
    for pain_point, data in pain_points.items():
        if pain_point in pain_point_mapping:
            mapping = pain_point_mapping[pain_point]
            
            recommendation = {
                'bank': bank_name if bank_name else 'All Banks',
                'category': 'Pain Point Resolution',
                'issue': mapping['issue'],
                'specific_recommendation': mapping['recommendations'][0],
                'additional_options': mapping['recommendations'][1:],
                'priority': mapping['priority'],
                'expected_impact': mapping['expected_impact'],
                'implementation_effort': mapping['effort'],
                'estimated_timeline': '1-3 months',
                'key_metric_to_track': 'Reduction in related complaints'
            }
            
            recommendations.append(recommendation)
    
    return recommendations

def generate_competitive_recommendations(comparison_results, bank_name):
    """Generate recommendations based on competitive positioning"""
    
    recommendations = []
    bank_data = comparison_results.get(bank_name, {})
    
    if not bank_data:
        return recommendations
    
    # Recommendations based on rating performance
    if bank_data.get('avg_rating', 0) < 3.5:
        recommendations.append({
            'bank': bank_name,
            'category': 'Competitive Improvement',
            'issue': f'Low average rating ({bank_data.get("avg_rating", 0):.2f}/5) compared to industry',
            'specific_recommendation': 'Implement customer feedback immediate resolution system',
            'additional_options': [
                'Launch customer satisfaction guarantee program',
                'Create rating improvement task force'
            ],
            'priority': 'High',
            'expected_impact': '0.5 point increase in average rating within 6 months',
            'implementation_effort': 'Medium',
            'estimated_timeline': '2-4 months',
            'key_metric_to_track': 'Average customer rating'
        })
    
    # Recommendations based on sentiment
    if bank_data.get('negative_ratio', 0) > 0.3:
        recommendations.append({
            'bank': bank_name,
            'category': 'Sentiment Improvement',
            'issue': f'High negative sentiment ratio ({bank_data.get("negative_ratio", 0):.1%})',
            'specific_recommendation': 'Proactive customer outreach program for dissatisfied customers',
            'additional_options': [
                'Implement sentiment analysis for real-time complaint detection',
                'Create negative feedback response team'
            ],
            'priority': 'High',
            'expected_impact': '20% reduction in negative sentiment within 3 months',
            'implementation_effort': 'Medium',
            'estimated_timeline': '1-2 months',
            'key_metric_to_track': 'Negative sentiment ratio'
        })
    
    # Capitalize on strengths
    strengths = bank_data.get('strengths', [])
    if strengths:
        recommendations.append({
            'bank': bank_name,
            'category': 'Strength Amplification',
            'issue': 'Opportunity to amplify existing strengths',
            'specific_recommendation': f'Marketing campaign highlighting: {", ".join(strengths[:2])}',
            'additional_options': [
                'Create case studies from positive customer experiences',
                'Develop referral program leveraging satisfied customers'
            ],
            'priority': 'Medium',
            'expected_impact': '15% increase in positive brand mentions',
            'implementation_effort': 'Low',
            'estimated_timeline': '1-2 months',
            'key_metric_to_track': 'Brand sentiment and referral rate'
        })
    
    return recommendations

def generate_innovation_recommendations(df, bank_name):
    """Generate innovative recommendations based on trends"""
    
    bank_reviews = df[df['bank_name'] == bank_name]
    
    # Analyze common themes in positive reviews
    positive_reviews = bank_reviews[bank_reviews['sentiment_category'] == 'positive']
    
    recommendations = []
    
    # Check for digital banking mentions
    digital_terms = ['app', 'online', 'mobile', 'digital', 'website']
    digital_mentions = positive_reviews['cleaned_content'].str.contains('|'.join(digital_terms), case=False).sum()
    
    if digital_mentions > 0 and len(positive_reviews) > 0:
        digital_ratio = digital_mentions / len(positive_reviews)
        
        if digital_ratio > 0.3:
            recommendations.append({
                'bank': bank_name,
                'category': 'Digital Innovation',
                'issue': 'Strong positive response to digital features',
                'specific_recommendation': 'Accelerate digital feature development based on praised aspects',
                'additional_options': [
                    'Introduce AI-powered financial insights in mobile app',
                    'Develop personalized banking experience using customer data',
                    'Create seamless omnichannel banking experience'
                ],
                'priority': 'Medium',
                'expected_impact': 'Increased customer engagement and retention',
                'implementation_effort': 'High',
                'estimated_timeline': '6-12 months',
                'key_metric_to_track': 'Digital engagement metrics and feature adoption rate'
            })
    
    return recommendations

def create_recommendation_matrix(recommendations):
    """Create a prioritized recommendation matrix"""
    
    if not recommendations:
        return None
    
    # Convert to DataFrame
    rec_df = pd.DataFrame(recommendations)
    
    # Calculate priority score
    priority_map = {'High': 3, 'Medium': 2, 'Low': 1}
    rec_df['priority_score'] = rec_df['priority'].map(priority_map)
    
    # Calculate effort score
    effort_map = {'Low': 1, 'Low-Medium': 1.5, 'Medium': 2, 'Medium-High': 2.5, 'High': 3}
    rec_df['effort_score'] = rec_df['implementation_effort'].map(effort_map)
    
    # Calculate ROI score (Priority/Effort)
    rec_df['roi_score'] = rec_df['priority_score'] / rec_df['effort_score']
    
    # Sort by ROI score
    rec_df = rec_df.sort_values('roi_score', ascending=False)
    
    # Add implementation phase
    def assign_phase(roi_score):
        if roi_score >= 2.0:
            return 'Phase 1: Immediate (0-3 months)'
        elif roi_score >= 1.0:
            return 'Phase 2: Short-term (3-6 months)'
        else:
            return 'Phase 3: Strategic (6-12 months)'
    
    rec_df['implementation_phase'] = rec_df['roi_score'].apply(assign_phase)
    
    # Save to CSV
    output_path = '../data/recommendation_matrix.csv'
    rec_df.to_csv(output_path, index=False)
    
    print(f"   ✓ Created prioritized recommendation matrix with {len(rec_df)} recommendations")
    
    return output_path

def generate_all_recommendations(driver_results, comparison_results):
    """Generate comprehensive recommendations for all banks"""
    
    all_recommendations = []
    
    # Get bank names from comparison results
    banks = list(comparison_results.keys())
    
    for bank in banks:
        # Generate pain point recommendations
        bank_pain_points = driver_results.get('pain_points', {})
        pain_point_recs = generate_recommendations_from_pain_points(bank_pain_points, bank)
        
        # Generate competitive recommendations
        competitive_recs = generate_competitive_recommendations(comparison_results, bank)
        
        # Combine recommendations
        bank_recommendations = pain_point_recs + competitive_recs
        
        # Add to master list
        all_recommendations.extend(bank_recommendations)
    
    return all_recommendations