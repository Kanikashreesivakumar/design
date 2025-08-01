import os
import sqlite3
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# SQLite DB path
SQLITE_DB = r"E:\kitkart(RNAIPL)\design\database.db"

# PostgreSQL config
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'project@2025'),
    'port': os.getenv('DB_PORT', '5432')
}

# List of tables to transfer
TABLES = ["rfid_log", "usernames", "uid_number", "repair_log"]

def transfer_table(table):
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_cur = sqlite_conn.cursor()
    sqlite_cur.execute(f"SELECT * FROM {table}")
    rows = sqlite_cur.fetchall()
    columns = [desc[0] for desc in sqlite_cur.description]

    pg_conn = psycopg2.connect(**DB_CONFIG)
    pg_cur = pg_conn.cursor()

    placeholders = ', '.join(['%s'] * len(columns))
    col_names = ', '.join([f'"{col}"' for col in columns])
    insert_stmt = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'

    for row in rows:
        pg_cur.execute(insert_stmt, row)

    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()
    sqlite_cur.close()
    sqlite_conn.close()
    print(f"Transferred {len(rows)} rows to {table}")

if __name__ == "__main__":
    # List tables in SQLite
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_cur = sqlite_conn.cursor()
    sqlite_cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print("Tables in your SQLite DB:", [row[0] for row in sqlite_cur.fetchall()])
    sqlite_cur.close()
    sqlite_conn.close()

    for table in TABLES:
        try:
            transfer_table(table)
        except Exception as e:
            print(f"Error transferring {table}: {e}")