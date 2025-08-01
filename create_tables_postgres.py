import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'project@2025'),
    'port': os.getenv('DB_PORT', '5432')
}

table_sql = {
    "rfid_log": """
        CREATE TABLE IF NOT EXISTS rfid_log (
            id SERIAL PRIMARY KEY,
            rfid TEXT,
            trolley_name TEXT,
            check_point TEXT,
            tpm_category TEXT,
            remarks TEXT,
            due_date DATE,
            previous_completed_date DATE
            -- Add more columns as needed
        );
    """,
    "usernames": """
        CREATE TABLE IF NOT EXISTS usernames (
            rfid TEXT PRIMARY KEY,
            name TEXT
        );
    """,
    "uid_number": """
        CREATE TABLE IF NOT EXISTS uid_number (
            uid TEXT PRIMARY KEY,
            trolley_id TEXT
        );
    """,
    "repair_log": """
        CREATE TABLE IF NOT EXISTS repair_log (
            id SERIAL PRIMARY KEY
            -- Add columns as needed for your repair_log.xlsx
        );
    """
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for name, sql in table_sql.items():
        cur.execute(sql)
        print(f"Table '{name}' created (if not exists).")
    conn.commit()
    cur.close()
    conn.close()
    print("All tables created successfully in PostgreSQL.")
except Exception as e:
    print("Error creating tables:", e)