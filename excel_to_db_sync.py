import pandas as pd
import sqlite3
import os
import time
from datetime import datetime

DB_FILE = 'database.db'

def sync_excel_to_database():
    """Sync Excel to Database - Run separately from website"""
    try:
        excel_path = r"G:\kitkart\REPAIR_LOG_LOCAL.xlsx"
        
        if not os.path.exists(excel_path):
            print(f"❌ Excel file not found: {excel_path}")
            return

        print(f"📁 Reading Excel file: {excel_path}")
        df_excel = pd.read_excel(excel_path, engine='openpyxl')

        if df_excel.empty:
            print("⚠️ Excel file is empty")
            return

        print(f"📊 Loaded {len(df_excel)} records from Excel")

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

        # Map Excel columns
        if 'Id' in df_excel.columns:
            df_excel['id'] = df_excel['Id'].fillna('').astype(str)
        elif 'id' in df_excel.columns:
            df_excel['id'] = df_excel['id'].fillna('').astype(str)
        else:
            print("❌ No 'Id' column found")
            conn.close()
            return

        # Map columns
        mapped_data = {}
        column_mappings = {
            'id': 'id',
            'name': 'name',
            'trolley number': 'trolley_number',
            'zone': 'zone',
            'concern': 'concern_description',
            'mobile number': 'email',
            'completion time': 'completion_time'
        }

        for excel_col in df_excel.columns:
            col_lower = excel_col.lower().strip()
            for excel_pattern, db_col in column_mappings.items():
                if excel_pattern in col_lower:
                    mapped_data[db_col] = df_excel[excel_col].fillna('').astype(str)
                    print(f"✅ Mapped '{excel_col}' → '{db_col}'")
                    break

        # Add missing columns
        required_columns = ['id', 'trolley_number', 'concern_description', 'completion_time',
                            'action_taken_by', 'action_time', 'action_status', 'email', 'name', 'zone']
        for col in required_columns:
            if col not in mapped_data:
                mapped_data[col] = [''] * len(df_excel)

        # Create final dataframe
        final_df = pd.DataFrame(mapped_data)

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
        final_df.to_sql('repair_log', conn, if_exists='replace', index=False)
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