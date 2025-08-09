# 📄 Filename: import_excel_to_postgres.py

import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2

# Load environment variables
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'project@2025'),
    'port': os.getenv('DB_PORT', '5432')
}

# Paths to Excel files
uid_file = r"G:\kitkart\New folder\uid_number.xlsx"
log_file = r"G:\kitkart\New folder\rfid_log.xlsx"
user_file = r"G:\kitkart\New folder\USERNAME.xlsx"
repair_log_file = r"G:\kitkart\New folder\REPAIR_LOG_LOCAL.xlsx"

def import_to_postgres(df, table_name, conn):
  
    cols = ', '.join([f'"{col}" TEXT' for col in df.columns])
    create_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols});'
    cur = conn.cursor()
    cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    cur.execute(create_stmt)
    
    for _, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        columns = ', '.join([f'"{col}"' for col in df.columns])
        insert_stmt = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'
        cur.execute(insert_stmt, tuple(row.astype(str)))
    conn.commit()
    cur.close()
    print(f" Imported: {table_name} → {len(df)} rows")

try:
    conn = psycopg2.connect(**DB_CONFIG)
except Exception as e:
    print(f"Could not connect to PostgreSQL: {e}")
    exit(1)

# Import uid_number.xlsx
try:
    uid_df = pd.read_excel(uid_file)
    uid_df.columns = [col.strip().lower().replace(" ", "_") for col in uid_df.columns]
    import_to_postgres(uid_df, "uid_number", conn)
except Exception as e:
    print(f" Failed to import uid_number: {e}")

# Import rfid_log.xlsx
try:
    rfid_df = pd.read_excel(log_file)
    rfid_df.columns = [col.strip().lower().replace(" ", "_") for col in rfid_df.columns]
    import_to_postgres(rfid_df, "rfid_log", conn)
except Exception as e:
    print(f" Failed to import rfid_log: {e}")

# Import USERNAME.xlsx
try:
    user_df = pd.read_excel(user_file)
    user_df.columns = [col.strip().lower().replace(" ", "_") for col in user_df.columns]
    import_to_postgres(user_df, "usernames", conn)
except Exception as e:
    print(f"Failed to import usernames: {e}")

# Import REPAIR_LOG_LOCAL.xlsx
try:
    repair_df = pd.read_excel(repair_log_file)
    repair_df.columns = [col.strip().lower().replace(" ", "_") for col in repair_df.columns]
    import_to_postgres(repair_df, "repair_log", conn)
except FileNotFoundError:
    print(f" File not found: {repair_log_file}")
except Exception as e:
    print(f" Failed to import repair_log: {e}")

conn.close()
print("All Excel files imported successfully into PostgreSQL.")
