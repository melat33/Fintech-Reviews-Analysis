"""
TASK 3: Store Cleaned Data in PostgreSQL
"""

import sys
import os
import psycopg2
import pandas as pd

print("🚀 STARTING TASK 3: PostgreSQL Database Setup")
print("=" * 60)

# OPTION A: Direct import from the file you know is correct
database_setup_path = r"D:\10 acadamy\fintech-reviews-analysis\fintech-customer-analytics\data_storage\database_setup.py"

# Import using importlib
import importlib.util
spec = importlib.util.spec_from_file_location("database_setup", database_setup_path)
database_setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(database_setup)

# Now import the class
DatabaseSetup = database_setup.DatabaseSetup

# OPTION B: Or use exec
# with open(database_setup_path, 'r') as f:
#     exec(f.read())

# Continue with your other imports
data_loader_path = r"D:\10 acadamy\fintech-reviews-analysis\fintech-customer-analytics\data_storage\data_loader.py"
spec2 = importlib.util.spec_from_file_location("data_loader", data_loader_path)
data_loader_module = importlib.util.module_from_spec(spec2)
spec2.loader.exec_module(data_loader_module)
DataLoader = data_loader_module.DataLoader

database_queries_path = r"D:\10 acadamy\fintech-reviews-analysis\fintech-customer-analytics\data_storage\database_queries.py"
spec3 = importlib.util.spec_from_file_location("database_queries", database_queries_path)
database_queries_module = importlib.util.module_from_spec(spec3)
spec3.loader.exec_module(database_queries_module)
DatabaseQueries = database_queries_module.DatabaseQueries

print("✅ All modules loaded from explicit paths!")

# Rest of your main() function remains the same...