# db_connect.py
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd

# Step 1: Load .env file
load_dotenv()

# Step 2: Read variables
server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
driver = os.getenv("DB_DRIVER")

# Step 3: Create connection string (Windows Authentication)
connection_string = f"mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver={quote_plus(driver)}&Encrypt=no&TrustServerCertificate=yes"


# Step 4: Create engine
engine = create_engine(connection_string)

# Step 5: Test the connection
try:
    with engine.connect() as conn:
        print("✅ Connection successful!")
        # Optional: show list of tables
        result = conn.exec_driver_sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES;").fetchall()
        print("\nTables in your database:")
        for row in result:
            print("-", row[0])
except Exception as e:
    print("❌ Connection failed:", e)
