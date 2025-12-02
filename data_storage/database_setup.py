# data_storage/database_setup.py
import psycopg2
import os
import sys

# Add config directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from database_config import DatabaseConfig

class DatabaseSetup:
    def __init__(self):
        self.db_config = DatabaseConfig.get_config()
        self.conn = None
    
    # ... rest of your code remains the same ...