"""
TASK 3: Store Cleaned Data in PostgreSQL
Complete database setup and data loading pipeline
"""

import sys
import os
import psycopg2
import pandas as pd

# Add the data_storage directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'data_storage'))

from database_setup import DatabaseSetup
from data_loader import DataLoader
from database_queries import DatabaseQueries

def check_postgresql_service():
    """Check if PostgreSQL service is running"""
    print("🔍 Checking PostgreSQL Service...")
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='postgres',
            user='postgres',
            password='melilove',
            port='5432',
            connect_timeout=5
        )
        conn.close()
        print("✅ PostgreSQL service is RUNNING")
        return True
    except Exception as e:
        print(f"❌ PostgreSQL service is NOT RUNNING: {e}")
        print("\n💡 Please start PostgreSQL service:")
        print("   1. Press Windows + R, type 'services.msc'")
        print("   2. Find 'PostgreSQL' service")
        print("   3. Right-click → Start")
        return False

def display_database_tables():
    """Display actual database tables and sample data"""
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='bank_reviews', 
            user='postgres',
            password='melilove',
            port='5432'
        )
        
        print("\n📋 DATABASE TABLES & SAMPLE DATA")
        print("=" * 50)
        
        # Show banks table
        print("\n🏦 BANKS TABLE:")
        banks_df = pd.read_sql("SELECT * FROM banks;", conn)
        print(banks_df.to_string(index=False))
        
        # Show reviews summary
        reviews_count = pd.read_sql("SELECT COUNT(*) as total_reviews FROM reviews;", conn)
        print(f"\n📝 TOTAL REVIEWS: {reviews_count.iloc[0]['total_reviews']}")
        
        # Show sample reviews for each bank
        for bank_name in banks_df['bank_name']:
            print(f"\n🔍 {bank_name.upper()} - SAMPLE REVIEWS:")
            sample_query = f"""
            SELECT review_text, rating, sentiment_label, sentiment_score 
            FROM reviews r 
            JOIN banks b ON r.bank_id = b.bank_id 
            WHERE b.bank_name = '{bank_name}' 
            LIMIT 2;
            """
            sample_df = pd.read_sql(sample_query, conn)
            print(sample_df.to_string(index=False))
        
        conn.close()
        print("\n✅ Database tables displayed successfully!")
        
    except Exception as e:
        print(f"❌ Error displaying database tables: {e}")

def main():
    print("🚀 STARTING TASK 3: PostgreSQL Database Setup")
    print("=" * 60)
    
    # Check if PostgreSQL is running first
    if not check_postgresql_service():
        print("\n❌ TASK 3 FAILED: PostgreSQL service is not running")
        print("💡 Please start PostgreSQL service and run this script again")
        return False
    
    try:
        # Step 1: Setup database
        print("\n📊 STEP 1: Database Setup")
        print("-" * 30)
        db_setup = DatabaseSetup()
        if not db_setup.setup_complete_database():
            print("❌ Database setup failed!")
            return False
        
        # Step 2: Load data
        print("\n📥 STEP 2: Data Loading") 
        print("-" * 30)
        data_loader = DataLoader()
        if not data_loader.load_all_data():
            print("❌ Data loading failed!")
            return False
        
        # Step 3: Verify data
        print("\n🔍 STEP 3: Data Verification")
        print("-" * 30)
        db_queries = DatabaseQueries()
        db_queries.run_verification_queries()
        
        # Step 4: Display database tables
        print("\n📋 STEP 4: Database Tables Display")
        print("-" * 30)
        display_database_tables()
        
        print("\n🎉 TASK 3 COMPLETED SUCCESSFULLY!")
        print("✅ PostgreSQL database created")
        print("✅ Tables: banks & reviews created") 
        print("✅ Data loaded from Task 2")
        print("✅ Verification queries executed")
        print("✅ Database tables displayed")
        print("✅ Ready for production use!")
        return True
        
    except Exception as e:
        print(f"\n❌ TASK 3 FAILED: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)