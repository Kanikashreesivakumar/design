"""
Check rfid_log table structure and sample data
"""
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'project@2025',
    'port': '5432'
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("\n" + "="*70)
    print("📊 RFID_LOG TABLE STRUCTURE")
    print("="*70)
    
    # Get column info
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = 'rfid_log' AND table_schema = 'public'
        ORDER BY ordinal_position
    """)
    
    columns = cur.fetchall()
    print("\n📋 Columns:")
    for col in columns:
        print(f"   - {col[0]:30} | {col[1]:15} | Nullable: {col[2]}")
    
    # Get sample data
    cur.execute("SELECT * FROM rfid_log LIMIT 3")
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]
    
    print("\n📄 Sample Data (3 rows):")
    print("   Columns:", col_names)
    for i, row in enumerate(rows, 1):
        print(f"\n   Row {i}:")
        for col_name, value in zip(col_names, row):
            print(f"      {col_name:25} = {value}")
    
    # Count total rows
    cur.execute("SELECT COUNT(*) FROM rfid_log")
    count = cur.fetchone()[0]
    print(f"\n✅ Total rows in rfid_log: {count}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
