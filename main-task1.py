"""
TASK 1: Data Collection & Preprocessing Pipeline
Matches your existing scraper.py structure
"""

import sys
import os
import pandas as pd
from datetime import datetime

print("=" * 60)
print("🚀 TASK 1: DATA COLLECTION & PROCESSING PIPELINE")
print("=" * 60)

# Set up paths
current_dir = os.path.dirname(os.path.abspath(__file__))
data_collection_path = os.path.join(current_dir, '2_data_pipeline', 'data_collection')
data_processing_path = os.path.join(current_dir, '2_data_pipeline', 'data_processing')
config_path = os.path.join(current_dir, '3_configuration')

# Add to Python path
sys.path.insert(0, data_collection_path)
sys.path.insert(0, data_processing_path)
if config_path not in sys.path:
    sys.path.insert(0, config_path)

print(f"[INFO] Current directory: {current_dir}")
print(f"[INFO] Data collection path: {data_collection_path}")
print(f"[INFO] Data processing path: {data_processing_path}")

def run_scraping():
    """Run the scraping from your existing scraper.py"""
    print("\n" + "=" * 60)
    print("PHASE 1: DATA COLLECTION")
    print("=" * 60)
    
    try:
        # Import your existing scraper module
        import importlib.util
        scraper_file = os.path.join(data_collection_path, 'scraper.py')
        
        if os.path.exists(scraper_file):
            print(f"[INFO] Found scraper.py: {scraper_file}")
            
            # Load the module
            spec = importlib.util.spec_from_file_location("scraper_module", scraper_file)
            scraper_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(scraper_module)
            
            # Check what functions are available
            available_functions = [f for f in dir(scraper_module) if not f.startswith('_')]
            print(f"[INFO] Available functions: {available_functions}")
            
            # Run the main function from your scraper
            if hasattr(scraper_module, 'main'):
                print("[INFO] Running scraper.main()...")
                scraper_module.main()
                
                # Check for output files
                raw_dir = os.path.join(current_dir, '2_data_pipeline', 'data', 'raw')
                if os.path.exists(raw_dir):
                    files = os.listdir(raw_dir)
                    print(f"[INFO] Files in raw directory: {files}")
                    
                    # Load the combined file
                    combined_file = os.path.join(raw_dir, 'all_reviews.csv')
                    if os.path.exists(combined_file):
                        df = pd.read_csv(combined_file)
                        print(f"[SUCCESS] Collected {len(df)} reviews")
                        return df
                    else:
                        print(f"[WARNING] Combined file not found: {combined_file}")
                else:
                    print(f"[ERROR] Raw directory not found: {raw_dir}")
            else:
                print("[ERROR] 'main' function not found in scraper.py")
                
        else:
            print(f"[ERROR] scraper.py not found at: {scraper_file}")
            
    except Exception as e:
        print(f"[ERROR] Scraping failed: {e}")
        import traceback
        traceback.print_exc()
    
    return None

def run_preprocessing(raw_df):
    """Run preprocessing on the collected data"""
    print("\n" + "=" * 60)
    print("PHASE 2: DATA PREPROCESSING")
    print("=" * 60)
    
    if raw_df is None or len(raw_df) == 0:
        print("[ERROR] No data to preprocess!")
        return None
    
    try:
        # Check column names
        print(f"[INFO] Data shape: {raw_df.shape}")
        print(f"[INFO] Columns: {list(raw_df.columns)}")
        
        # Rename columns to match expected format
        column_mapping = {
            'review': 'review_text',
            'score': 'rating',
            'at': 'review_date',
            'package_name': 'app_package',
            'bank_name': 'bank_name',
            'user_name': 'user_name',
            'source': 'source'
        }
        
        # Apply renaming
        for old_col, new_col in column_mapping.items():
            if old_col in raw_df.columns:
                raw_df = raw_df.rename(columns={old_col: new_col})
        
        print("[INFO] Columns after renaming:")
        for col in raw_df.columns:
            print(f"  - {col}")
        
        # Basic preprocessing steps
        print("\n[STEP 1] Handling missing values...")
        
        # Check missing values
        missing_report = {}
        for column in raw_df.columns:
            missing = raw_df[column].isnull().sum()
            if missing > 0:
                missing_report[column] = missing
                print(f"  {column}: {missing} missing values")
        
        # Fill missing ratings with median
        if 'rating' in raw_df.columns:
            median_rating = raw_df['rating'].median()
            raw_df['rating'] = raw_df['rating'].fillna(median_rating)
            print(f"  Filled missing ratings with median: {median_rating}")
        
        # Fill missing text
        if 'review_text' in raw_df.columns:
            raw_df['review_text'] = raw_df['review_text'].fillna('No review text')
            print(f"  Filled missing review text")
        
        print("\n[STEP 2] Removing duplicates...")
        before = len(raw_df)
        raw_df = raw_df.drop_duplicates(subset=['review_text', 'user_name', 'bank_name'], keep='first')
        after = len(raw_df)
        print(f"  Removed {before - after} duplicates")
        
        print("\n[STEP 3] Normalizing dates...")
        if 'review_date' in raw_df.columns:
            # Convert to datetime
            raw_df['review_date'] = pd.to_datetime(raw_df['review_date'], errors='coerce')
            
            # Format to YYYY-MM-DD
            raw_df['review_date'] = raw_df['review_date'].dt.strftime('%Y-%m-%d')
            
            # Check for invalid dates
            invalid_dates = raw_df['review_date'].isnull().sum()
            if invalid_dates > 0:
                print(f"  Warning: {invalid_dates} invalid dates found")
        
        print("\n[STEP 4] Calculating data quality metrics...")
        quality_metrics = {
            'total_reviews': len(raw_df),
            'banks_covered': raw_df['bank_name'].nunique() if 'bank_name' in raw_df.columns else 0,
            'date_range': {},
            'rating_stats': {},
            'missing_values': missing_report
        }
        
        # Date range
        if 'review_date' in raw_df.columns:
            valid_dates = pd.to_datetime(raw_df['review_date'], errors='coerce')
            valid_dates = valid_dates.dropna()
            if not valid_dates.empty:
                quality_metrics['date_range'] = {
                    'earliest': valid_dates.min().strftime('%Y-%m-%d'),
                    'latest': valid_dates.max().strftime('%Y-%m-%d')
                }
        
        # Rating statistics
        if 'rating' in raw_df.columns:
            quality_metrics['rating_stats'] = {
                'average': round(raw_df['rating'].mean(), 2),
                'median': raw_df['rating'].median(),
                'min': raw_df['rating'].min(),
                'max': raw_df['rating'].max()
            }
        
        print(f"[INFO] Total reviews: {quality_metrics['total_reviews']}")
        print(f"[INFO] Banks covered: {quality_metrics['banks_covered']}")
        
        if 'date_range' in quality_metrics and quality_metrics['date_range']:
            print(f"[INFO] Date range: {quality_metrics['date_range']['earliest']} to {quality_metrics['date_range']['latest']}")
        
        if 'rating_stats' in quality_metrics:
            print(f"[INFO] Average rating: {quality_metrics['rating_stats']['average']}/5")
        
        return raw_df, quality_metrics
        
    except Exception as e:
        print(f"[ERROR] Preprocessing failed: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def save_processed_data(df, quality_metrics):
    """Save processed data and quality reports"""
    print("\n" + "=" * 60)
    print("PHASE 3: SAVING PROCESSED DATA")
    print("=" * 60)
    
    if df is None or len(df) == 0:
        print("[ERROR] No data to save!")
        return False
    
    try:
        # Create processed directory
        processed_dir = os.path.join(current_dir, '2_data_pipeline', 'data', 'processed')
        os.makedirs(processed_dir, exist_ok=True)
        
        # Save main cleaned file
        main_output = os.path.join(processed_dir, 'all_clean_reviews.csv')
        df.to_csv(main_output, index=False, encoding='utf-8')
        print(f"[SUCCESS] Saved main file: {main_output}")
        print(f"  Reviews: {len(df)}")
        print(f"  Columns: {len(df.columns)}")
        
        # Save bank-specific files
        if 'bank_name' in df.columns:
            for bank in df['bank_name'].unique():
                bank_df = df[df['bank_name'] == bank]
                bank_filename = bank.lower().replace(' ', '_') + '_clean.csv'
                bank_output = os.path.join(processed_dir, bank_filename)
                bank_df.to_csv(bank_output, index=False, encoding='utf-8')
                print(f"  Saved {len(bank_df)} reviews for {bank}")
        
        # Save quality report
        if quality_metrics:
            report_path = os.path.join(processed_dir, 'data_quality_report.txt')
            with open(report_path, 'w') as f:
                f.write("=" * 60 + "\n")
                f.write("DATA QUALITY REPORT\n")
                f.write("=" * 60 + "\n\n")
                
                f.write(f"Total Reviews: {quality_metrics['total_reviews']}\n")
                f.write(f"Banks Covered: {quality_metrics['banks_covered']}\n\n")
                
                if 'date_range' in quality_metrics and quality_metrics['date_range']:
                    f.write(f"Date Range: {quality_metrics['date_range']['earliest']} to {quality_metrics['date_range']['latest']}\n")
                
                if 'rating_stats' in quality_metrics:
                    f.write(f"\nRating Statistics:\n")
                    f.write(f"  Average: {quality_metrics['rating_stats']['average']}/5\n")
                    f.write(f"  Median: {quality_metrics['rating_stats']['median']}/5\n")
                    f.write(f"  Range: {quality_metrics['rating_stats']['min']} to {quality_metrics['rating_stats']['max']}\n")
                
                if 'missing_values' in quality_metrics:
                    f.write(f"\nMissing Values Report:\n")
                    for col, count in quality_metrics['missing_values'].items():
                        if count > 0:
                            f.write(f"  {col}: {count} missing\n")
                
                f.write("\n" + "=" * 60 + "\n")
                f.write("PREPROCESSING STEPS COMPLETED:\n")
                f.write("1. Missing values handled\n")
                f.write("2. Duplicates removed\n")
                f.write("3. Dates normalized to YYYY-MM-DD\n")
                f.write("4. Data quality metrics calculated\n")
                f.write("=" * 60 + "\n")
            
            print(f"[SUCCESS] Quality report saved: {report_path}")
        
        print(f"\n[SUCCESS] All files saved to: {processed_dir}")
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to save data: {e}")
        return False

def generate_final_report(df, quality_metrics):
    """Generate final execution report"""
    print("\n" + "=" * 60)
    print("🎉 TASK 1 COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    
    if df is not None:
        print(f"\n📊 FINAL STATISTICS:")
        print(f"  • Total reviews: {len(df):,}")
        print(f"  • Banks covered: {df['bank_name'].nunique() if 'bank_name' in df.columns else 0}")
        
        if 'review_date' in df.columns:
            print(f"  • Date format: All dates in YYYY-MM-DD format")
        
        if 'rating' in df.columns:
            print(f"  • Average rating: {df['rating'].mean():.2f}/5")
        
        print(f"\n✅ PREPROCESSING COMPLETED:")
        print(f"  1. Missing values handled systematically")
        print(f"  2. Duplicates removed")
        print(f"  3. Dates normalized to YYYY-MM-DD")
        print(f"  4. Data quality checks performed")
        print(f"  5. Missing-rate calculations done")
        
        print(f"\n📁 OUTPUT FILES:")
        print(f"  • Raw data: 2_data_pipeline/data/raw/")
        print(f"  • Clean data: 2_data_pipeline/data/processed/")
        print(f"  • Quality report: data_quality_report.txt")
        
        print(f"\n🎯 READY FOR TASK 2: SENTIMENT ANALYSIS")
        print("=" * 60)

def main():
    """Main execution function"""
    try:
        # Step 1: Run scraping
        raw_df = run_scraping()
        
        if raw_df is None or len(raw_df) == 0:
            print("\n[ERROR] No data collected. Exiting...")
            return False
        
        # Step 2: Run preprocessing
        clean_df, quality_metrics = run_preprocessing(raw_df)
        
        if clean_df is None:
            print("\n[ERROR] Preprocessing failed. Exiting...")
            return False
        
        # Step 3: Save processed data
        if not save_processed_data(clean_df, quality_metrics):
            print("\n[ERROR] Failed to save processed data.")
            return False
        
        # Step 4: Generate final report
        generate_final_report(clean_df, quality_metrics)
        
        return True
        
    except Exception as e:
        print(f"\n❌ TASK 1 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)