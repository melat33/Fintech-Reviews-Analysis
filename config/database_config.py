# config/database_config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DatabaseConfig:
    """Centralized database configuration"""
    
    @staticmethod
    def get_config():
        """Get database configuration from environment variables"""
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'bank_reviews'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD'),  # Required, no default
            'port': os.getenv('DB_PORT', '5432')
        }
    
    @staticmethod
    def validate_config():
        """Validate that all required config values are present"""
        config = DatabaseConfig.get_config()
        if not config['password']:
            raise ValueError("❌ DB_PASSWORD is not set in environment variables!")
        return True