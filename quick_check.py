"""
Quick check - what's actually in the rfid_log table?
"""
import psycopg2

conn = psycopg2.connect(
    host='localhost',
    database='postgres',
    user='postgres',
    password='project@2025',
    port='5432'
)

cur = conn.cursor()
cur.execute("SELECT id, uid, trolley_name, tpm_category FROM rfid_log LIMIT 5")
rows = cur.fetchall()

print("\n=== RFID_LOG SAMPLE DATA ===")
print("ID | UID | Trolley Name | TPM Category")
print("-" * 60)
for row in rows:
    print(f"{row[0]} | {row[1][:20] if row[1] else 'None'} | {row[2] if row[2] else 'None'} | {row[3] if row[3] else 'None'}")

cur.execute("SELECT COUNT(*) FROM rfid_log WHERE id IS NOT NULL")
count = cur.fetchone()[0]
print(f"\nTotal records with non-null ID: {count}")

cur.close()
conn.close()
