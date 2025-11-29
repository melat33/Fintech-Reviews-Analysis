"""
Configuration file for Bank Reviews Analysis Project
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Google Play Store App IDs
APP_IDS = {
    'CBE': os.getenv('CBE_APP_ID', 'com.combanketh.mobilebanking'),
    'Zemen': os.getenv('ZEMEN_APP_ID', 'com.ZemenBank.MobileApp'),
    'Abyssinia': os.getenv('BOA_APP_ID', 'com.boa.boaMobileBanking'),
    'Dashen': os.getenv('DASHEN_APP_ID', 'com.dashen.dashensuperapp'),
    'Abay': os.getenv('ABAY_APP_ID', 'com.ground360.abaybank'),
}

# Bank Names Mapping
BANK_NAMES = {
    'CBE': 'Commercial Bank of Ethiopia',
    'Zemen': 'Zemen Bank',
    'Abyssinia': 'Bank of Abyssinia',
    'Dashen': 'Dashen Bank',
    'Abay': 'Abay (Abaye) Bank'
}

# Scraping config
SCRAPING_CONFIG = {
    'reviews_per_bank': int(os.getenv('REVIEWS_PER_BANK', 400)),
    'max_retries': int(os.getenv('MAX_RETRIES', 3)),
    'lang': 'en',
    'country': 'et'
}

# File paths
DATA_PATHS = {
    'raw': 'data/raw',
    'processed': 'data/processed',
    'raw_reviews': 'data/raw/reviews_raw.csv',
    'processed_reviews': 'data/processed/reviews_processed.csv'
}
