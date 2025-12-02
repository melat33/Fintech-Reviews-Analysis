# data_storage/data_loader.py
import psycopg2
import pandas as pd
from datetime import datetime
import sys
import os

# Add config directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from database_config import DatabaseConfig

class DataLoader:
    def __init__(self):
        self.db_config = DatabaseConfig.get_config()
        self.conn = None
    
    # ... rest of your code remains the same ...