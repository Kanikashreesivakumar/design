

import sqlite3
import pandas as pd
from pathlib import Path


uid_file = r"G:\kitkart\New folder\uid_number.xlsx"
log_file = r"G:\kitkart\New folder\rfid_log.xlsx"
user_file = r"G:\kitkart\New folder\USERNAME.xlsx"
repair_log_file = r"G:\kitkart\New folder\REPAIR_LOG_LOCAL.xlsx" 


conn = sqlite3.connect("database.db")


try:
    uid_df = pd.read_excel(uid_file)
    uid_df.columns = [col.strip().lower().replace(" ", "_") for col in uid_df.columns]
    uid_df.to_sql("uid_number", conn, if_exists="replace", index=False)
    print(f" Imported: uid_number → {len(uid_df)} rows")
except Exception as e:
    print(f" Failed to import uid_number: {e}")


try:
    rfid_df = pd.read_excel(log_file)
    rfid_df.columns = [col.strip().lower().replace(" ", "_") for col in rfid_df.columns]
    rfid_df.to_sql("rfid_log", conn, if_exists="replace", index=False)
    print(f" Imported: rfid_log → {len(rfid_df)} rows")
except Exception as e:
    print(f" Failed to import rfid_log: {e}")

try:
    user_df = pd.read_excel(user_file)
    user_df.columns = [col.strip().lower().replace(" ", "_") for col in user_df.columns]
    user_df.to_sql("usernames", conn, if_exists="replace", index=False)
    print(f"Imported: usernames → {len(user_df)} rows")
except Exception as e:
    print(f" Failed to import usernames: {e}")

try:
    repair_df = pd.read_excel(repair_log_file)
    repair_df.columns = [col.strip().lower().replace(" ", "_") for col in repair_df.columns]
    repair_df.to_sql("repair_log", conn, if_exists="replace", index=False)
    print(f" Imported: repair_log → {len(repair_df)} rows")
except FileNotFoundError:
    print(f" File not found: {repair_log_file}")
except Exception as e:
    print(f" Failed to import repair_log: {e}")

conn.close()
print(" All Excel files imported successfully into database.db")
