# data_processing/data_cleaning.py - ENHANCED VERSION
import pandas as pd
import numpy as np
import re
from preprocessing import DataPreprocessor

class DataCleaner:
    def __init__(self):
        self.preprocessor = DataPreprocessor()
        self.cleaning_log = []
    
    def standardize_bank_names(self, df):
        """Standardize bank names to ensure consistency"""
        bank_name_mapping = {
            'cbe': 'Commercial Bank of Ethiopia',
            'CBE': 'Commercial Bank of Ethiopia',
            'Commercial Bank': 'Commercial Bank of Ethiopia',
            'commercial bank of ethiopia': 'Commercial Bank of Ethiopia',
            
            'boa': 'Bank of Abyssinia',
            'BOA': 'Bank of Abyssinia',
            'Bank of abyssinia': 'Bank of Abyssinia',
            
            'dashen': 'Dashen Bank',
            'Dashen': 'Dashen Bank',
            
            'zemen': 'Zemen Bank',
            'Zemen': 'Zemen Bank',
            
            'abay': 'Abay Bank',
            'Abay': 'Abay Bank'
        }
        
        if 'bank_name' in df.columns:
            original_names = df['bank_name'].unique()
            df['bank_name'] = df['bank_name'].map(
                lambda x: bank_name_mapping.get(str(x).lower().strip(), x)
            )
            
            changes = len([1 for orig in original_names if orig != bank_name_mapping.get(str(orig).lower().strip(), orig)])
            self.cleaning_log.append(f"Standardized {changes} bank names")
            
        return df
    
    def clean_text_content(self, df):
        """Clean review text content"""
        if 'review_text' not in df.columns:
            return df
        
        def clean_text(text):
            if pd.isna(text):
                return text
            
            text = str(text)
            # Remove excessive whitespace
            text = re.sub(r'\s+', ' ', text)
            # Remove special characters but keep basic punctuation
            text = re.sub(r'[^\w\s.,!?\-]', '', text)
            # Trim
            text = text.strip()
            return text
        
        df['review_text_cleaned'] = df['review_text'].apply(clean_text)
        
        # Flag empty or very short reviews
        df['text_length'] = df['review_text_cleaned'].str.len()
        short_reviews = (df['text_length'] < 10).sum()
        
        if short_reviews > 0:
            self.cleaning_log.append(f"Found {short_reviews} reviews with less than 10 characters")
        
        return df
    
    def validate_ratings(self, df):
        """Validate and clean rating values"""
        if 'rating' not in df.columns:
            return df
        
        # Convert to numeric, coercing errors to NaN
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
        
        # Ensure ratings are between 1-5
        invalid_ratings = df['rating'].isna().sum()
        out_of_range = ((df['rating'] < 1) | (df['rating'] > 5)).sum()
        
        if invalid_ratings > 0:
            self.cleaning_log.append(f"Found {invalid_ratings} invalid ratings (converted to NaN)")
        
        if out_of_range > 0:
            # Clip ratings to valid range
            df['rating'] = df['rating'].clip(1, 5)
            self.cleaning_log.append(f"Clipped {out_of_range} ratings to valid range (1-5)")
        
        return df
    
    def add_data_quality_flags(self, df):
        """Add flags for data quality issues"""
        # Flag for missing critical fields
        df['missing_text_flag'] = df['review_text'].isna() | (df['review_text'].str.strip() == '')
        df['missing_rating_flag'] = df['rating'].isna()
        
        # Flag for suspiciously short reviews
        df['short_text_flag'] = df['review_text'].str.len() < 20
        
        # Flag for duplicate content (simple check)
        text_counts = df['review_text'].value_counts()
        duplicate_texts = text_counts[text_counts > 1].index.tolist()
        df['potential_duplicate_flag'] = df['review_text'].isin(duplicate_texts)
        
        # Count flags
        flags_summary = {
            'missing_text': df['missing_text_flag'].sum(),
            'missing_rating': df['missing_rating_flag'].sum(),
            'short_text': df['short_text_flag'].sum(),
            'potential_duplicates': df['potential_duplicate_flag'].sum()
        }
        
        self.cleaning_log.append(f"Data quality flags added: {flags_summary}")
        
        return df, flags_summary
    
    def clean_pipeline(self, df):
        """
        Complete data cleaning pipeline
        """
        print("[INFO] Starting data cleaning pipeline...")
        
        original_count = len(df)
        
        # Step 1: Standardize bank names
        print("\n[STEP 1] Standardizing bank names...")
        df = self.standardize_bank_names(df)
        
        # Step 2: Clean text content
        print("\n[STEP 2] Cleaning text content...")
        df = self.clean_text_content(df)
        
        # Step 3: Validate ratings
        print("\n[STEP 3] Validating ratings...")
        df = self.validate_ratings(df)
        
        # Step 4: Add data quality flags
        print("\n[STEP 4] Adding data quality flags...")
        df, flags_summary = self.add_data_quality_flags(df)
        
        # Step 5: Run preprocessing pipeline
        print("\n[STEP 5] Running preprocessing pipeline...")
        df, quality_report = self.preprocessor.preprocess_pipeline(df)
        
        # Log summary
        final_count = len(df)
        removed_count = original_count - final_count
        
        print("\n" + "=" * 60)
        print("CLEANING PIPELINE COMPLETE")
        print("=" * 60)
        print(f"Original reviews: {original_count}")
        print(f"Final reviews: {final_count}")
        print(f"Removed during cleaning: {removed_count}")
        
        if self.cleaning_log:
            print("\nCleaning actions performed:")
            for log in self.cleaning_log:
                print(f"  • {log}")
        
        print("\n" + "=" * 60)
        
        return df, quality_report