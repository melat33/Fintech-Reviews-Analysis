# data_loader.py
import psycopg2
import pandas as pd
from datetime import datetime

class DataLoader:
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
        try:
            self.conn = psycopg2.connect(**self.db_config)
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def load_cleaned_data(self):
        try:
            data_path = "2_data_pipeline/data/processed/all_sentiment_reviews.csv"
            df = pd.read_csv(data_path)
            print(f"Loaded {len(df)} reviews from Task 2")
            return df
        except Exception as e:
            print(f"Could not load data: {e}")
            return None

    def insert_banks(self):
        ethiopian_banks = [
            ("Bank of Abyssinia", "BoA Mobile"),
            ("Commercial Bank of Ethiopia", "CBE Mobile"),
            ("Dashen Bank", "Dashen Mobile"),
            ("Zemen Bank", "Zemen Mobile"),
            ("Abay Bank", "Abay Mobile")
        ]
        try:
            with self.conn.cursor() as cursor:
                for bank_name, app_name in ethiopian_banks:
                    cursor.execute(
                        "INSERT INTO banks (bank_name, app_name) VALUES (%s, %s) ON CONFLICT (bank_name) DO NOTHING",
                        (bank_name, app_name)
                    )
                self.conn.commit()
                print(f"Inserted {len(ethiopian_banks)} banks")
                return True
        except Exception as e:
            print(f"Bank insertion failed: {e}")
            self.conn.rollback()
            return False

    def get_bank_mapping(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT bank_id, bank_name FROM banks")
                return {row[1]: row[0] for row in cursor.fetchall()}
        except Exception as e:
            print(f"Failed to get bank mapping: {e}")
            return {}

    def insert_reviews(self, df):
        bank_mapping = self.get_bank_mapping()
        if not bank_mapping:
            print("No banks found in database!")
            return False
        inserted_count = 0
        try:
            with self.conn.cursor() as cursor:
                for index, row in df.iterrows():
                    bank_name = row['bank_name']
                    bank_id = bank_mapping.get(bank_name)
                    if bank_id:
                        review_id = f"review_{index}_{hash(str(row['review_text'])) % 10000}"
                        cursor.execute("""
                            INSERT INTO reviews 
                            (review_id, bank_id, review_text, rating, review_date, sentiment_label, sentiment_score, source)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (review_id) DO NOTHING
                        """, (
                            review_id,
                            bank_id,
                            row['review_text'],
                            row['rating'],
                            datetime.now(),
                            row.get('ensemble_label', 'neutral'),
                            row.get('vader_score', 0.0),
                            'Google Play'
                        ))
                        if cursor.rowcount > 0:
                            inserted_count += 1
                self.conn.commit()
                print(f"Inserted {inserted_count} reviews into database")
                return True
        except Exception as e:
            print(f"Review insertion failed: {e}")
            self.conn.rollback()
            return False

    def load_all_data(self):
        print("Starting data loading process...")
        if not self.connect():
            return False
        df = self.load_cleaned_data()
        if df is None:
            return False
        if not self.insert_banks():
            return False
        if not self.insert_reviews(df):
            return False
        print("Data loading completed successfully!")
        return True