import psycopg2

class DatabaseQueries:
    def __init__(self):
        # UPDATE PASSWORD TO YOUR POSTGRES PASSWORD
        self.db_config = {
            'host': 'localhost',
            'database': 'bank_reviews',
            'user': 'postgres',
            'password': 'melilove',  # CHANGE THIS
            'port': '5432'
        }
        self.conn = None
    
    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_config)
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def run_verification_queries(self):
        """Run queries to verify data integrity"""
        queries = {
            "Total Reviews": "SELECT COUNT(*) FROM reviews;",
            "Reviews per Bank": """
                SELECT b.bank_name, COUNT(r.review_id) as review_count 
                FROM banks b LEFT JOIN reviews r ON b.bank_id = r.bank_id 
                GROUP BY b.bank_name ORDER BY review_count DESC;
            """,
            "Average Rating per Bank": """
                SELECT b.bank_name, ROUND(AVG(r.rating), 2) as avg_rating
                FROM banks b JOIN reviews r ON b.bank_id = r.bank_id 
                GROUP BY b.bank_name ORDER BY avg_rating DESC;
            """,
            "Sentiment Distribution": """
                SELECT sentiment_label, COUNT(*) as count 
                FROM reviews GROUP BY sentiment_label ORDER BY count DESC;
            """
        }
        
        if not self.connect():
            return
        
        print("🔍 RUNNING VERIFICATION QUERIES")
        print("=" * 50)
        
        for query_name, query in queries.items():
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                    print(f"\n📊 {query_name}:")
                    for row in results:
                        print(f"   {row}")
            except Exception as e:
                print(f"❌ Query failed ({query_name}): {e}")

if __name__ == "__main__":
    queries = DatabaseQueries()
    queries.run_verification_queries()