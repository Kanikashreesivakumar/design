import os
import pandas as pd
from pathlib import Path
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

def import_dataframe_to_postgres(df, table_name, conn):
    # Drop and recreate table
    cols = ', '.join([f'"{col}" TEXT' for col in df.columns])
    create_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols});'
    cur = conn.cursor()
    cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    cur.execute(create_stmt)
    # Insert data
    for _, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        insert_stmt = f'INSERT INTO "{table_name}" ({", ".join([f\'"{col}"\' for col in df.columns])}) VALUES ({placeholders})'
        cur.execute(insert_stmt, tuple(row.astype(str)))
    conn.commit()
    cur.close()
    print(f"Imported: {table_name} → {len(df)} rows")

try:
    conn = psycopg2.connect(**DB_CONFIG)
except Exception as e:
    print(f"❌ Could not connect to PostgreSQL: {e}")
    exit(1)

# ========== TABLE 1: UID NUMBER.xlsx ==========
uid_file = "UID NUMBER.xlsx"
if Path(uid_file).exists():
    uid_df = pd.read_excel(uid_file, engine='openpyxl')
    uid_df.columns = [col.strip().lower().replace(" ", "_") for col in uid_df.columns]
    if 'uid' in uid_df.columns and 'trolley_id' in uid_df.columns:
        import_dataframe_to_postgres(uid_df[['uid', 'trolley_id']], "uid_number", conn)
    else:
        print(f" Required columns not found in {uid_file}")
else:
    print(f" File not found: {uid_file}")

# ========== TABLE 2: rfid_log.xlsx ==========
rfid_log_file = "rfid_log.xlsx"
if Path(rfid_log_file).exists():
    rfid_df = pd.read_excel(rfid_log_file, engine='openpyxl')
    rfid_df.columns = [col.strip().lower().replace(" ", "_") for col in rfid_df.columns]
    import_dataframe_to_postgres(rfid_df, "rfid_log", conn)
else:
    print(f" File not found: {rfid_log_file}")

# ========== TABLE 3: USERNAME.xlsx ==========
username_file = "USERNAME.xlsx"
if Path(username_file).exists():
    user_df = pd.read_excel(username_file, engine='openpyxl')
    user_df.columns = [col.strip().lower().replace(" ", "_") for col in user_df.columns]
    import_dataframe_to_postgres(user_df, "usernames", conn)
else:
    print(f" File not found: {username_file}")

# ========== TABLE 4: REPAIR_LOG_LOCAL.xlsx ==========
repair_file = r"G:\kitkart\New folder\REPAIR_LOG_LOCAL.xlsx"
if Path(repair_file).exists():
    try:
        xls = pd.ExcelFile(repair_file, engine='openpyxl')
        sheet_imported = False

        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet, engine='openpyxl')
            print(f"Sheet: {sheet} → {df.shape[0]} rows, {df.shape[1]} columns")

            if df.shape[1] > 0:
                df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
                import_dataframe_to_postgres(df, "repair_log", conn)
                sheet_imported = True
                break

        if not sheet_imported:
            print(" No usable sheets found in REPAIR_LOG_LOCAL.xlsx")

    except Exception as e:
        print(f" Error reading REPAIR_LOG_LOCAL.xlsx: {e}")
else:
    print(f"File not found: {repair_file}")

conn.close()
print("All Excel files imported successfully into PostgreSQL.")
