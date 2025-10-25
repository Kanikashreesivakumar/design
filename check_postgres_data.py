"""
Check PostgreSQL database tables and row counts
"""
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'project@2025'),
    'port': os.getenv('DB_PORT', '5432')
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("\n" + "="*60)
    print("📊 POSTGRESQL DATABASE STATUS")
    print("="*60)
    
    # Get all tables
    cur.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]
    
    total_rows = 0
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        total_rows += count
        
        # Get sample data
        cur.execute(f"SELECT * FROM {table} LIMIT 3")
        sample = cur.fetchall()
        
        print(f"\n📋 Table: {table}")
        print(f"   Rows: {count}")
        if count > 0 and sample:
            print(f"   Sample: {len(sample)} rows shown")
            cols = [desc[0] for desc in cur.description]
            print(f"   Columns: {', '.join(cols)}")
    
    print("\n" + "="*60)
    print(f"✅ Total tables: {len(tables)}")
    print(f"✅ Total rows: {total_rows}")
    print("="*60)
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
