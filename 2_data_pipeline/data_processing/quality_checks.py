# data_processing/quality_checks.py - NEW FILE
import pandas as pd
import numpy as np

class DataQualityChecker:
    def __init__(self):
        self.checks_passed = 0
        self.checks_total = 0
    
    def check_completeness(self, df, critical_columns=['review_text', 'rating', 'bank_name']):
        """Check data completeness for critical columns"""
        results = {}
        
        for column in critical_columns:
            if column in df.columns:
                missing = df[column].isnull().sum()
                total = len(df)
                completeness = (total - missing) / total if total > 0 else 0
                
                results[column] = {
                    'missing': missing,
                    'total': total,
                    'completeness': round(completeness * 100, 2),
                    'status': 'PASS' if completeness >= 0.95 else 'FAIL'
                }
        
        self.checks_total += 1
        if all(r['status'] == 'PASS' for r in results.values()):
            self.checks_passed += 1
        
        return {'completeness': results}
    
    def check_duplicates(self, df, subset=['review_text', 'user_name', 'bank_name']):
        """Check for duplicate records"""
        duplicates = df.duplicated(subset=subset).sum()
        duplicate_rate = duplicates / len(df) if len(df) > 0 else 0
        
        result = {
            'duplicate_count': int(duplicates),
            'total_records': len(df),
            'duplicate_rate': round(duplicate_rate * 100, 2),
            'status': 'PASS' if duplicate_rate <= 0.05 else 'FAIL'
        }
        
        self.checks_total += 1
        if result['status'] == 'PASS':
            self.checks_passed += 1
        
        return {'duplicates': result}
    
    def check_date_format(self, df, date_column='review_date'):
        """Validate date format is YYYY-MM-DD"""
        if date_column not in df.columns:
            return {'date_format': {'status': 'SKIP', 'reason': f'Column {date_column} not found'}}
        
        # Check if dates match YYYY-MM-DD pattern
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        valid_dates = df[date_column].astype(str).str.match(date_pattern).sum()
        invalid_dates = len(df) - valid_dates
        
        result = {
            'valid_dates': int(valid_dates),
            'invalid_dates': int(invalid_dates),
            'valid_percentage': round(valid_dates / len(df) * 100, 2) if len(df) > 0 else 0,
            'status': 'PASS' if invalid_dates == 0 else 'FAIL'
        }
        
        self.checks_total += 1
        if result['status'] == 'PASS':
            self.checks_passed += 1
        
        return {'date_format': result}
    
    def check_rating_range(self, df, rating_column='rating'):
        """Check that ratings are within valid range (1-5)"""
        if rating_column not in df.columns:
            return {'rating_range': {'status': 'SKIP', 'reason': f'Column {rating_column} not found'}}
        
        valid_ratings = df[rating_column].between(1, 5).sum()
        invalid_ratings = len(df) - valid_ratings
        
        result = {
            'valid_ratings': int(valid_ratings),
            'invalid_ratings': int(invalid_ratings),
            'valid_percentage': round(valid_ratings / len(df) * 100, 2) if len(df) > 0 else 0,
            'status': 'PASS' if invalid_ratings == 0 else 'FAIL'
        }
        
        self.checks_total += 1
        if result['status'] == 'PASS':
            self.checks_passed += 1
        
        return {'rating_range': result}
    
    def check_bank_coverage(self, df, expected_banks=5):
        """Check that all expected banks are covered"""
        if 'bank_name' not in df.columns:
            return {'bank_coverage': {'status': 'SKIP', 'reason': 'bank_name column not found'}}
        
        unique_banks = df['bank_name'].nunique()
        bank_list = df['bank_name'].unique().tolist()
        
        result = {
            'unique_banks': int(unique_banks),
            'expected_banks': expected_banks,
            'bank_list': bank_list,
            'status': 'PASS' if unique_banks >= expected_banks else 'FAIL'
        }
        
        self.checks_total += 1
        if result['status'] == 'PASS':
            self.checks_passed += 1
        
        return {'bank_coverage': result}
    
    def calculate_missing_rates(self, df):
        """Calculate missing rates for all columns"""
        missing_rates = {}
        
        for column in df.columns:
            missing_count = df[column].isnull().sum()
            total = len(df)
            missing_rate = (missing_count / total * 100) if total > 0 else 0
            
            missing_rates[column] = {
                'missing_count': int(missing_count),
                'total': total,
                'missing_rate': round(missing_rate, 2),
                'severity': 'HIGH' if missing_rate > 20 else 'MEDIUM' if missing_rate > 5 else 'LOW'
            }
        
        return {'missing_rates': missing_rates}
    
    def run_all_checks(self, df):
        """Run all quality checks"""
        print("[INFO] Running comprehensive quality checks...")
        
        all_results = {}
        
        # Run individual checks
        all_results.update(self.check_completeness(df))
        all_results.update(self.check_duplicates(df))
        all_results.update(self.check_date_format(df))
        all_results.update(self.check_rating_range(df))
        all_results.update(self.check_bank_coverage(df))
        all_results.update(self.calculate_missing_rates(df))
        
        # Calculate overall score
        overall_score = (self.checks_passed / self.checks_total * 100) if self.checks_total > 0 else 0
        
        all_results['overall'] = {
            'checks_passed': self.checks_passed,
            'checks_total': self.checks_total,
            'score': round(overall_score, 2),
            'status': 'PASS' if overall_score >= 90 else 'FAIL'
        }
        
        return all_results
    
    def generate_quality_report(self, quality_results):
        """Generate a comprehensive quality report"""
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("DATA QUALITY ASSURANCE REPORT")
        report_lines.append("=" * 60)
        
        # Overall score
        if 'overall' in quality_results:
            overall = quality_results['overall']
            report_lines.append(f"\n📊 OVERALL QUALITY SCORE: {overall['score']}%")
            report_lines.append(f"   Checks passed: {overall['checks_passed']}/{overall['checks_total']}")
            report_lines.append(f"   Status: {overall['status']}")
        
        # Detailed checks
        for check_name, check_result in quality_results.items():
            if check_name == 'overall':
                continue
            
            report_lines.append(f"\n🔍 {check_name.upper().replace('_', ' ')}:")
            
            if isinstance(check_result, dict) and 'status' in check_result:
                # Single check result
                for key, value in check_result.items():
                    if key != 'status':
                        report_lines.append(f"   {key}: {value}")
                report_lines.append(f"   Status: {check_result['status']}")
            elif isinstance(check_result, dict):
                # Multiple results (like missing_rates)
                for item_name, item_details in check_result.items():
                    if isinstance(item_details, dict):
                        report_lines.append(f"   {item_name}:")
                        for k, v in item_details.items():
                            report_lines.append(f"     {k}: {v}")
        
        # Recommendations
        report_lines.append("\n" + "=" * 60)
        report_lines.append("RECOMMENDATIONS")
        report_lines.append("=" * 60)
        
        if 'missing_rates' in quality_results:
            high_missing = []
            for col, stats in quality_results['missing_rates'].items():
                if stats['severity'] == 'HIGH':
                    high_missing.append(f"{col} ({stats['missing_rate']}% missing)")
            
            if high_missing:
                report_lines.append("\n⚠️  HIGH PRIORITY ISSUES:")
                for issue in high_missing:
                    report_lines.append(f"  • {issue}")
        
        if 'date_format' in quality_results and quality_results['date_format']['status'] == 'FAIL':
            report_lines.append("\n📅 DATE FORMAT ISSUES:")
            report_lines.append(f"  • {quality_results['date_format']['invalid_dates']} invalid dates found")
            report_lines.append("  • Ensure all dates are in YYYY-MM-DD format")
        
        report_lines.append("\n" + "=" * 60)
        
        return "\n".join(report_lines)