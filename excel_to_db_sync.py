import pandas as pd
import sqlite3
import os
import time
from datetime import datetime
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
DB_FILE = str(APP_DIR / 'database.db')

def sync_excel_to_database():
    """Sync Excel to Database - upsert repair rows by id while preserving actions."""
    try:
        excel_path = r"E:\kitkart\design\REPAIR_LOG_LOCAL.xlsx"
        
        if not os.path.exists(excel_path):
            print(f"❌ Excel file not found: {excel_path}")
            return

        print(f"📁 Reading Excel file: {excel_path}")
        with pd.ExcelFile(excel_path, engine='openpyxl') as xls:
            df_excel = pd.DataFrame()
            for sheet in xls.sheet_names:
                candidate = pd.read_excel(xls, sheet_name=sheet, engine='openpyxl')
                if not candidate.empty:
                    print(f"📄 Using sheet: {sheet} → {len(candidate)} rows")
                    df_excel = candidate
                    break

        if df_excel.empty:
            print("⚠️ Excel file is empty")
            return

        print(f"📊 Loaded {len(df_excel)} records from Excel")

        df_excel = df_excel.copy()
        df_excel.columns = [str(col).strip().lower() for col in df_excel.columns]

        if 'id' not in df_excel.columns:
            print("❌ No 'Id' column found")
            return

        df_excel['id'] = df_excel['id'].fillna('').astype(str).str.strip()
        df_excel = df_excel[df_excel['id'] != '']
        if df_excel.empty:
            print("⚠️ No valid repair-log ids found in Excel")
            return

        # Connect to SQLite
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        # Get existing action data (preserve it)
        existing_actions = {}
        try:
            c.execute('SELECT id, action_taken_by, action_time, action_status FROM repair_log WHERE action_taken_by IS NOT NULL AND action_taken_by != ""')
            for row in c.fetchall():
                existing_actions[str(row[0])] = {
                    'action_taken_by': row[1],
                    'action_time': row[2],
                    'action_status': row[3]
                }
            print(f"💾 Preserving {len(existing_actions)} existing action records")
        except Exception as e:
            print(f"⚠️ Could not read existing action data: {e}")

        def pick_column(possible_names):
            for name in possible_names:
                if name in df_excel.columns:
                    return df_excel[name].fillna('').astype(str)
            return pd.Series([''] * len(df_excel), index=df_excel.index)

        final_df = pd.DataFrame({
            'id': df_excel['id'],
            'trolley_number': pick_column(['trolley number', 'trolley_number', 'trolley no', 'trolley']),
            'concern_description': pick_column(['concern description', 'concern', 'concern_description']),
            'completion_time': pick_column(['completion time', 'completion_time']),
            'action_taken_by': pick_column(['action taken by', 'action_taken_by']),
            'action_time': pick_column(['action time', 'action_time']),
            'action_status': pick_column(['action status', 'action_status']),
            'email': pick_column(['mobile number', 'email']),
            'name': pick_column(['name']),
            'zone': pick_column(['zone']),
        })

        # Preserve action data
        preserved = 0
        for idx, row in final_df.iterrows():
            rid = str(row['id']).strip()
            if rid in existing_actions:
                action = existing_actions[rid]
                if action['action_taken_by'] and str(action['action_taken_by']).strip():
                    final_df.at[idx, 'action_taken_by'] = action['action_taken_by']
                    final_df.at[idx, 'action_time'] = action['action_time']
                    final_df.at[idx, 'action_status'] = action['action_status']
                    preserved += 1
                    print(f"✅ Preserved action for ID {rid}: {action['action_taken_by']}")

        print(f"✅ Preserved action data for {preserved} records")

        # Clean and save to database
        final_df = final_df.replace(['None', 'nan', 'NaT', 'null'], '').fillna('')
        upsert_columns = ['id', 'trolley_number', 'concern_description', 'completion_time',
                          'action_taken_by', 'action_time', 'action_status', 'email', 'name', 'zone']
        placeholders = ', '.join(['?'] * len(upsert_columns))
        update_clause = ', '.join([f'{col}=excluded.{col}' for col in upsert_columns if col != 'id'])
        insert_sql = (
            f"INSERT INTO repair_log ({', '.join(upsert_columns)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT(id) DO UPDATE SET {update_clause}"
        )
        rows_to_write = [tuple(final_df[col].iloc[idx] for col in upsert_columns) for idx in range(len(final_df))]
        c.executemany(insert_sql, rows_to_write)
        conn.commit()
        conn.close()

        print(f"✅ Synced {len(final_df)} records to database successfully")

    except Exception as e:
        print(f"❌ Sync failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    print("🔄 Starting Excel to Database sync...")
    sync_excel_to_database()
    print("✅ Sync completed!")