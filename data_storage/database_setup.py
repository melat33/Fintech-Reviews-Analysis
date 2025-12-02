# data_storage/database_setup.py
import psycopg2
import os

class DatabaseSetup:
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'database': 'bank_reviews',
            'user': 'postgres',
            'password': 'melilove',
            'port': '5432'
        }
        self.conn = None
    
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            print("✅ Connected to PostgreSQL successfully!")
            return True
        except psycopg2.OperationalError as e:
            print(f"❌ Connection failed: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False
    
    def create_database(self):
        """Create database if it doesn't exist"""
        try:
            temp_config = self.db_config.copy()
            temp_config['database'] = 'postgres'
            conn = psycopg2.connect(**temp_config)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE bank_reviews;")
            print("✅ Database 'bank_reviews' created!")
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"ℹ️  Database might already exist: {e}")
            return True
    
    def create_tables(self):
        """Create banks and reviews tables using schema file"""
        try:
            schema_path = os.path.join(os.path.dirname(__file__), 'schema', 'bank_reviews_schema.sql')
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            
            with self.conn.cursor() as cursor:
                cursor.execute(schema_sql)
                self.conn.commit()
                print("✅ Tables 'banks' and 'reviews' created successfully!")
                return True
        except Exception as e:
            print(f"❌ Table creation failed: {e}")
            self.conn.rollback()
            return False
    
    def check_tables_exist(self):
        """Check if tables already exist"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('banks', 'reviews');
                """)
                existing_tables = [row[0] for row in cursor.fetchall()]
                return len(existing_tables) == 2
        except Exception as e:
            print(f"❌ Error checking tables: {e}")
            return False
    
    def setup_complete_database(self):
        """Run complete database setup"""
        print("🚀 Starting PostgreSQL Database Setup...")
        
        # First try to connect to existing database
        if self.connect():
            if self.check_tables_exist():
                print("✅ Database and tables already exist!")
                return True
            else:
                print("📊 Creating tables...")
                return self.create_tables()
        else:
            # If connection fails, try to create database
            print("📝 Database doesn't exist, creating it...")
            if self.create_database():
                # Now connect to new database and create tables
                if self.connect() and self.create_tables():
                    print("🎉 Database setup completed successfully!")
                    return True
                else:
                    print("❌ Failed to create tables in new database!")
                    return False
            else:
                print("❌ Could not create database!")
                return False

if __name__ == "__main__":
    db_setup = DatabaseSetup()
    db_setup.setup_complete_database()