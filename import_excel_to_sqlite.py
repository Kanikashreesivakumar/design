import sqlite3
import pandas as pd
from pathlib import Path

# Step 1: Create or connect to SQLite database
conn = sqlite3.connect("database.db")  # Creates if not exists

# Step 2: Function to insert DataFrame into SQLite table
def insert_dataframe_to_table(df, table_name, conn):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Imported: {table_name} → {len(df)} rows")

# ========== TABLE 1: UID NUMBER.xlsx ==========
uid_file = "UID NUMBER.xlsx"
if Path(uid_file).exists():
    uid_df = pd.read_excel(uid_file, engine='openpyxl')
    uid_df.columns = [col.strip().lower().replace(" ", "_") for col in uid_df.columns]
    
    if 'uid' in uid_df.columns and 'trolley_id' in uid_df.columns:
        insert_dataframe_to_table(uid_df[['uid', 'trolley_id']], "uid_number", conn)
    else:
        print(f" Required columns not found in {uid_file}")
else:
    print(f" File not found: {uid_file}")

# ========== TABLE 2: rfid_log.xlsx ==========
rfid_log_file = "rfid_log.xlsx"
if Path(rfid_log_file).exists():
    rfid_df = pd.read_excel(rfid_log_file, engine='openpyxl')
    rfid_df.columns = [col.strip().lower().replace(" ", "_") for col in rfid_df.columns]
    insert_dataframe_to_table(rfid_df, "rfid_log", conn)
else:
    print(f" File not found: {rfid_log_file}")

# ========== TABLE 3: USERNAME.xlsx ==========
username_file = "USERNAME.xlsx"
if Path(username_file).exists():
    user_df = pd.read_excel(username_file, engine='openpyxl')
    user_df.columns = [col.strip().lower().replace(" ", "_") for col in user_df.columns]
    insert_dataframe_to_table(user_df, "usernames", conn)
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

            # Accept even if there are 0 rows but valid columns
            if df.shape[1] > 0:
                df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
                insert_dataframe_to_table(df, "repair_log", conn)
                sheet_imported = True
                break

        if not sheet_imported:
            print(" No usable sheets found in REPAIR_LOG_LOCAL.xlsx")

    except Exception as e:
        print(f" Error reading REPAIR_LOG_LOCAL.xlsx: {e}")
else:
    print(f"File not found: {repair_file}")


conn.close()
print(" All Excel files imported successfully into database.db")
