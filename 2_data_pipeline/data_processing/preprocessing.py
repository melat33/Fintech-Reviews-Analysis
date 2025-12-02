# data_processing/preprocessing.py - ENHANCED VERSION
import pandas as pd
import numpy as np
from datetime import datetime
import re

class DataPreprocessor:
    def __init__(self):
        self.quality_report = {}
    
    def normalize_dates(self, df, date_column='review_date'):
        """
        Convert dates to YYYY-MM-DD format
        Handles multiple date formats
        """
        if date_column not in df.columns:
            print(f"[WARNING] Date column '{date_column}' not found")
            return df
        
        original_dates = df[date_column].copy()
        normalized_dates = []
        date_formats_found = []
        
        for date_str in df[date_column]:
            if pd.isna(date_str):
                normalized_dates.append(None)
                continue
                
            # Try multiple date formats
            date_formats = [
                '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', 
                '%Y/%m/%d', '%b %d, %Y', '%B %d, %Y', '%d %b %Y'
            ]
            
            parsed_date = None
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(str(date_str).strip(), fmt)
                    if fmt not in date_formats_found:
                        date_formats_found.append(fmt)
                    break
                except:
                    continue
            
            if parsed_date:
                normalized_dates.append(parsed_date.strftime('%Y-%m-%d'))
            else:
                # Try to extract date from string
                match = re.search(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', str(date_str))
                if match:
                    year, month, day = match.groups()
                    try:
                        parsed_date = datetime(int(year), int(month), int(day))
                        normalized_dates.append(parsed_date.strftime('%Y-%m-%d'))
                    except:
                        normalized_dates.append(None)
                else:
                    normalized_dates.append(None)
        
        df[date_column] = normalized_dates
        
        # Quality metrics
        valid_dates = sum(1 for d in normalized_dates if d is not None)
        self.quality_report['date_normalization'] = {
            'total': len(df),
            'valid_dates': valid_dates,
            'invalid_dates': len(df) - valid_dates,
            'date_formats_found': date_formats_found
        }
        
        print(f"[INFO] Date normalization: {valid_dates}/{len(df)} valid dates")
        if date_formats_found:
            print(f"[INFO] Date formats detected: {date_formats_found}")
        
        return df
    
    def remove_duplicates(self, df, subset_columns=['review_text', 'user_name', 'bank_name']):
        """
        Remove duplicate reviews based on text content and metadata
        """
        original_count = len(df)
        
        # Create text hash for better duplicate detection
        df['text_hash'] = df['review_text'].apply(
            lambda x: hash(str(x).lower().strip()[:500]) if pd.notna(x) else None
        )
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['text_hash', 'bank_name'], keep='first')
        
        # Remove the temporary column
        if 'text_hash' in df.columns:
            df = df.drop(columns=['text_hash'])
        
        duplicates_removed = original_count - len(df)
        
        self.quality_report['duplicate_removal'] = {
            'original_count': original_count,
            'final_count': len(df),
            'duplicates_removed': duplicates_removed,
            'duplicate_rate': (duplicates_removed / original_count * 100) if original_count > 0 else 0
        }
        
        print(f"[INFO] Duplicate removal: Removed {duplicates_removed} duplicates")
        print(f"[INFO] Remaining reviews: {len(df)}")
        
        return df
    
    def handle_missing_values(self, df):
        """
        Systematically handle and flag missing values
        """
        missing_report = {}
        
        for column in df.columns:
            missing_count = df[column].isnull().sum()
            missing_percentage = (missing_count / len(df)) * 100
            
            missing_report[column] = {
                'missing_count': missing_count,
                'missing_percentage': round(missing_percentage, 2),
                'total_values': len(df)
            }
            
            # Apply specific handling based on column
            if missing_count > 0:
                if column == 'rating':
                    # Fill missing ratings with median
                    median_rating = df[column].median()
                    df[column] = df[column].fillna(median_rating)
                    print(f"[INFO] Filled {missing_count} missing ratings with median: {median_rating}")
                
                elif column == 'review_text':
                    # Flag but don't fill (critical field)
                    df[f'{column}_missing_flag'] = df[column].isnull()
                    print(f"[WARNING] {missing_count} reviews have missing text (flagged)")
                
                elif column in ['user_name', 'review_date']:
                    # Fill with placeholder
                    placeholder = 'Unknown' if column == 'user_name' else '0000-00-00'
                    df[column] = df[column].fillna(placeholder)
                    print(f"[INFO] Filled {missing_count} missing {column} with '{placeholder}'")
        
        self.quality_report['missing_values'] = missing_report
        
        # Print summary
        print("\n[INFO] Missing Value Report:")
        for col, stats in missing_report.items():
            if stats['missing_count'] > 0:
                print(f"  {col}: {stats['missing_count']} missing ({stats['missing_percentage']}%)")
        
        return df
    
    def calculate_data_quality_metrics(self, df):
        """
        Calculate comprehensive data quality metrics
        """
        metrics = {
            'total_reviews': len(df),
            'banks_covered': df['bank_name'].nunique() if 'bank_name' in df.columns else 0,
            'date_range': {},
            'rating_distribution': {},
            'text_length_stats': {}
        }
        
        # Date range
        if 'review_date' in df.columns:
            valid_dates = pd.to_datetime(df['review_date'], errors='coerce')
            valid_dates = valid_dates.dropna()
            if not valid_dates.empty:
                metrics['date_range'] = {
                    'earliest': valid_dates.min().strftime('%Y-%m-%d'),
                    'latest': valid_dates.max().strftime('%Y-%m-%d'),
                    'span_days': (valid_dates.max() - valid_dates.min()).days
                }
        
        # Rating distribution
        if 'rating' in df.columns:
            rating_counts = df['rating'].value_counts().to_dict()
            metrics['rating_distribution'] = {
                'average': round(df['rating'].mean(), 2),
                'median': df['rating'].median(),
                'distribution': rating_counts
            }
        
        # Text length statistics
        if 'review_text' in df.columns:
            df['text_length'] = df['review_text'].str.len()
            metrics['text_length_stats'] = {
                'avg_length': round(df['text_length'].mean(), 0),
                'min_length': df['text_length'].min(),
                'max_length': df['text_length'].max(),
                'reviews_with_text': df['review_text'].notnull().sum()
            }
        
        self.quality_report['quality_metrics'] = metrics
        return metrics
    
    def generate_quality_report(self):
        """Generate a comprehensive quality report"""
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("DATA QUALITY REPORT")
        report_lines.append("=" * 60)
        
        if 'duplicate_removal' in self.quality_report:
            dup = self.quality_report['duplicate_removal']
            report_lines.append(f"\n📊 DUPLICATE REMOVAL:")
            report_lines.append(f"  Original reviews: {dup['original_count']}")
            report_lines.append(f"  Final reviews: {dup['final_count']}")
            report_lines.append(f"  Duplicates removed: {dup['duplicates_removed']}")
            report_lines.append(f"  Duplicate rate: {dup['duplicate_rate']:.2f}%")
        
        if 'missing_values' in self.quality_report:
            report_lines.append(f"\n📊 MISSING VALUES:")
            for col, stats in self.quality_report['missing_values'].items():
                if stats['missing_count'] > 0:
                    report_lines.append(f"  {col}: {stats['missing_count']} missing ({stats['missing_percentage']}%)")
        
        if 'quality_metrics' in self.quality_report:
            metrics = self.quality_report['quality_metrics']
            report_lines.append(f"\n📊 OVERVIEW METRICS:")
            report_lines.append(f"  Total reviews: {metrics['total_reviews']}")
            report_lines.append(f"  Banks covered: {metrics['banks_covered']}")
            
            if 'date_range' in metrics and metrics['date_range']:
                report_lines.append(f"  Date range: {metrics['date_range']['earliest']} to {metrics['date_range']['latest']}")
                report_lines.append(f"  Time span: {metrics['date_range']['span_days']} days")
            
            if 'rating_distribution' in metrics:
                report_lines.append(f"  Average rating: {metrics['rating_distribution']['average']}/5")
        
        report_lines.append("\n" + "=" * 60)
        
        return "\n".join(report_lines)
    
    def preprocess_pipeline(self, df):
        """
        Complete preprocessing pipeline
        """
        print("[INFO] Starting data preprocessing pipeline...")
        
        # Step 1: Handle missing values
        print("\n[STEP 1] Handling missing values...")
        df = self.handle_missing_values(df)
        
        # Step 2: Normalize dates
        print("\n[STEP 2] Normalizing dates...")
        df = self.normalize_dates(df)
        
        # Step 3: Remove duplicates
        print("\n[STEP 3] Removing duplicates...")
        df = self.remove_duplicates(df)
        
        # Step 4: Calculate quality metrics
        print("\n[STEP 4] Calculating quality metrics...")
        self.calculate_data_quality_metrics(df)
        
        # Step 5: Generate report
        print("\n[STEP 5] Generating quality report...")
        report = self.generate_quality_report()
        print(report)
        
        # Save quality report to file
        report_path = "../data/processed/data_quality_report.txt"
        with open(report_path, 'w') as f:
            f.write(report)
        print(f"[SUCCESS] Quality report saved to: {report_path}")
        
        return df, self.quality_report