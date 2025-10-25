"""
Test PostgreSQL Migration - Quick Summary
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

print("\n" + "="*70)
print("🔍 CHECKING POSTGRESQL DATA MIGRATION")
print("="*70)

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    tables = ['rfid_log', 'uid_number', 'usernames', 'repair_log']
    
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        
        status = "✅" if count > 0 else "⚠️"
        print(f"{status} {table:15} → {count:5} rows")
    
    print("="*70)
    print("\n✅ Migration verification complete!")
    print("\n💡 Your website should now show data at:")
    print("   📄 http://localhost:5000/records")
    print("   📄 http://localhost:5000/repair_log")
    print("   📊 http://localhost:5000/dashboard")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
