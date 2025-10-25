"""
SQLite to PostgreSQL Migration Script
This script transfers all data from SQLite database files to PostgreSQL
"""

import sqlite3
import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'project@2025'),
    'port': os.getenv('DB_PORT', '5432')
}

# SQLite database files
SQLITE_FILES = {
    'database.db': ['rfid_log', 'uid_number', 'usernames', 'repair_log'],
    'rfid_log.db': ['rfid_log'],
    'trolley_database.db': ['uid_number', 'usernames']
}

def get_sqlite_tables(db_path):
    """Get list of tables in SQLite database"""
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cur.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        print(f"❌ Error reading {db_path}: {e}")
        return []

def get_table_data(db_path, table_name):
    """Get all data from a SQLite table"""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()
        columns = [description[0] for description in cur.description]
        data = [dict(row) for row in rows]
        conn.close()
        return columns, data
    except Exception as e:
        print(f"❌ Error reading table {table_name}: {e}")
        return [], []

def insert_into_postgres(pg_conn, table_name, columns, data):
    """Insert data into PostgreSQL table"""
    if not data:
        print(f"⚠️  No data to insert into {table_name}")
        return 0
    
    cur = pg_conn.cursor()
    
    # Get PostgreSQL table columns
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND table_schema = 'public'
        ORDER BY ordinal_position
    """)
    pg_columns = [row[0] for row in cur.fetchall()]
    
    if not pg_columns:
        print(f"❌ Table {table_name} doesn't exist in PostgreSQL")
        cur.close()
        return 0
    
    # Find matching columns - include 'id' for tables with data
    if table_name in ['rfid_log', 'repair_log']:
        # Include ID for these tables to preserve record IDs
        matching_cols = [col for col in columns if col.lower() in pg_columns]
    else:
        # Exclude ID for tables with PRIMARY KEY constraints (uid_number, usernames)
        matching_cols = [col for col in columns if col.lower() in pg_columns and col.lower() != 'id']
    
    if not matching_cols:
        print(f"❌ No matching columns for {table_name}")
        print(f"   SQLite columns: {columns}")
        print(f"   PostgreSQL columns: {pg_columns}")
        cur.close()
        return 0
    
    # Clear existing data
    cur.execute(f"DELETE FROM {table_name}")
    print(f"🗑️  Cleared existing data from {table_name}")
    
    # Prepare insert statement with ON CONFLICT handling
    cols_str = ', '.join(matching_cols)
    placeholders = ', '.join(['%s'] * len(matching_cols))
    
    # Handle duplicates based on table primary key
    if table_name == 'uid_number':
        insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT (uid) DO NOTHING"
    elif table_name == 'usernames':
        insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT (rfid) DO NOTHING"
    elif table_name == 'rfid_log':
        insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
    elif table_name == 'repair_log':
        insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
    else:
        insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
    
    # Prepare data tuples (filter out rows with NULL values in key columns)
    data_tuples = []
    skipped = 0
    for row in data:
        tuple_data = tuple(row.get(col) for col in matching_cols)
        
        # Skip rows with NULL values in primary/important columns
        if table_name == 'uid_number' and (not tuple_data[0] or tuple_data[0] == 'None'):
            skipped += 1
            continue
        if table_name == 'usernames' and (not tuple_data[0] or tuple_data[0] == 'None'):
            skipped += 1
            continue
        
        data_tuples.append(tuple_data)
    
    if skipped > 0:
        print(f"   ⚠️  Skipped {skipped} rows with NULL/empty key values")
    
    # Execute batch insert
    if data_tuples:
        execute_batch(cur, insert_sql, data_tuples, page_size=100)
        pg_conn.commit()
        
        # Reset sequence for tables with SERIAL primary key
        if table_name in ['rfid_log', 'repair_log']:
            try:
                cur.execute(f"SELECT setval(pg_get_serial_sequence('{table_name}', 'id'), (SELECT COALESCE(MAX(id::integer), 1) FROM {table_name}))")
                print(f"   🔄 Reset ID sequence for {table_name}")
            except Exception as e:
                print(f"   ⚠️  Could not reset sequence: {e}")
        
        cur.close()
        return len(data_tuples)
    else:
        print(f"   ⚠️  No valid data to insert after filtering")
        pg_conn.commit()
        cur.close()
        return 0

def migrate_database(sqlite_path, tables_to_migrate, pg_conn):
    """Migrate specific tables from SQLite to PostgreSQL"""
    print(f"\n{'='*70}")
    print(f"📂 Processing: {sqlite_path}")
    print(f"{'='*70}")
    
    if not os.path.exists(sqlite_path):
        print(f"❌ File not found: {sqlite_path}")
        return
    
    # Get all tables in SQLite database
    all_tables = get_sqlite_tables(sqlite_path)
    print(f"📋 Available tables: {all_tables}")
    
    # Migrate each table
    for table in all_tables:
        if table in tables_to_migrate:
            print(f"\n📊 Migrating table: {table}")
            columns, data = get_table_data(sqlite_path, table)
            
            if data:
                print(f"   Columns: {columns}")
                print(f"   Rows: {len(data)}")
                
                inserted = insert_into_postgres(pg_conn, table, columns, data)
                if inserted > 0:
                    print(f"   ✅ Successfully migrated {inserted} rows to {table}")
            else:
                print(f"   ⚠️  No data found in {table}")

def main():
    print("\n" + "="*70)
    print("🚀 SQLite to PostgreSQL Migration Script")
    print("="*70)
    
    # Connect to PostgreSQL
    try:
        pg_conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connected to PostgreSQL")
    except Exception as e:
        print(f"❌ Could not connect to PostgreSQL: {e}")
        return
    
    # Migrate each SQLite database
    base_path = r"E:\kitkart(RNAIPL)\design"
    
    # Priority order: migrate from the main database.db first
    priority_files = ['database.db', 'rfid_log.db', 'trolley_database.db']
    
    for sqlite_file in priority_files:
        sqlite_path = os.path.join(base_path, sqlite_file)
        if os.path.exists(sqlite_path):
            # Determine which tables to migrate from this file
            if sqlite_file == 'database.db':
                tables = ['rfid_log', 'uid_number', 'usernames', 'repair_log']
            elif sqlite_file == 'rfid_log.db':
                tables = ['rfid_log']
            elif sqlite_file == 'trolley_database.db':
                tables = ['uid_number', 'usernames']
            else:
                tables = []
            
            migrate_database(sqlite_path, tables, pg_conn)
    
    pg_conn.close()
    
    print("\n" + "="*70)
    print("✅ MIGRATION COMPLETED!")
    print("="*70)
    print("\n💡 Next steps:")
    print("   1. Run: python database.py")
    print("   2. Open: http://localhost:5000/records")
    print("   3. Open: http://localhost:5000/repair_log")

if __name__ == "__main__":
    main()
