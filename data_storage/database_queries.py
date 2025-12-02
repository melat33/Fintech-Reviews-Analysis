# data_storage/database_queries.py
import psycopg2
import sys
import os

# Add config directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from database_config import DatabaseConfig

class DatabaseQueries:
    def __init__(self):
        self.db_config = DatabaseConfig.get_config()
        self.conn = None
    
    # ... rest of your code remains the same ...