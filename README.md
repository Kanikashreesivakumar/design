# KITKART (Flask + SQLite) — Project Documentation

## What this project does (overall)

This project is a Flask web application used to track trolley TPM activity using RFID scans and manual data entry, store everything in a local SQLite database (`database.db`), and present operational views (Records, Dashboard, Repair Log, Reports). RFID scans are received via a built-in TCP socket server and written into the `rfid_log` table; users can then update each record with TPM category (Primary/Complete checks or Repair), compute and store due dates based on the completed date, and capture checkpoint/concern/action details. Separately, the app maintains a `repair_log` table that can be synced from an Excel repair log (either from a configured file path or an uploaded file) while preserving any “action taken” fields already recorded inside SQLite. The UI is implemented with Jinja2 templates under `templates/`, and the app also contains optional email-notification logic (due-soon alerts and repair pending alerts) with JSON files used to avoid duplicate notifications.

## How to run

- Create and activate a virtual environment.
- Install dependencies from `requirements.txt`.
- Run the app **from the `design/` directory**, because `database.py` uses relative paths like `database.db`.

Example (PowerShell):

```powershell
cd e:\kitkart\design
python -m pip install -r requirements.txt
python database.py
```

Then open:
- `http://localhost:5000/records`

## Data model (SQLite)

The app primarily uses `database.db` with these tables:

- `rfid_log`: RFID scan and TPM records (UID, entry/exit times, trolley name, TPM category, due dates, checkpoint/concern/action fields).
- `uid_number`: UID → trolley_id mapping (used to resolve trolley name automatically when a UID is scanned).
- `usernames`: RFID → person name mapping (used on Repair Log “Scan RFID” to fill technician name).
- `repair_log`: Repair concerns synced from Excel or created manually via “Repair Entry”; technicians can submit an “action taken” update per repair record.

## Web routes and UI pages

All routes live in `database.py`.

- `/` → redirects to `/records`
- `/records` → main table view of `rfid_log` (filter by trolley prefix, edit latest TPM category).
- `/update_record/<id>` → detailed edit form for a single `rfid_log` row (sets exit time, checkpoint/concern/action fields, and due-date logic).
- `/dashboard` → charts and counts computed from `rfid_log`.
- `/reports` → summary counts and “due in next 7 days” list.
- `/repair-log` → repair log table view (synced from Excel before display) with modal form to “Take Action”.
- `/submit-action` (POST) → updates a repair record (action_taken/action_taken_by/action_time/action_status).
- `/repair-entry` → add a new repair_log row manually.
- `/new-record` → add a new rfid_log row manually.
- `/download_excel` → export `rfid_log` to an Excel download (optional prefix filter).
- `/import-export` → export + repair-log sync actions (sync-from-path and upload-to-sync).
- `/settings` → configure repair Excel path stored in `settings.json`.
- `/get_username/<rfid>` → AJAX helper returning the mapped username.
- `/get_tpm_counts` → AJAX helper for dashboard counts.
- `/api/repair-log-count` → returns count of pending repair actions.
- `/debug-excel`, `/debug-sqlite` → debug pages for troubleshooting Excel/SQLite contents.

## File-by-file explanation (everything under `design/`)

### Application code

- `database.py`
  - The main Flask application.
  - Owns DB access helpers (`insert_rfid_log`, `get_all_rfid_logs`, etc.), all web routes, due-date computation, Repair Log syncing, and the optional RFID TCP socket server.
  - **Important behavior**:
    - Starts a TCP server on `0.0.0.0:9000` (RFID reader sends raw bytes; UID is parsed from `data.hex().upper()`).
    - Enforces a 24-hour “cooldown” for re-scanning the same UID (new entry blocked if last entry is within 24h).
    - When a record is updated via `/update_record/<id>`, it writes `exit_date`/`exit_time`, concatenates checkpoint/concern/action lists into comma-separated strings, and sets `due_date` based on category:
      - Primary Check / Complete Check → due date = completed date + 90 days
      - Complete Check For Synchro → due date = completed date + 180 days
      - Repair → due date preserved from latest check if available, else defaults to 7 days
    - Repair log syncing is “add NEW rows only” in the current in-app sync code (`sync_repair_df_to_sqlite`), so technician actions already stored in SQLite are preserved.

- `excel_to_db_sync.py`
  - A standalone script to sync the repair Excel file into `database.db`.
  - Unlike the in-app sync (which appends new rows), this script uses `to_sql(..., if_exists='replace')`, but it **tries to preserve** existing action columns by reading them first and reapplying them before writing.
  - Intended to be run separately (not called by the Flask app).

- `import_excel_to_sqlite.py`
  - A standalone “initial import” script that reads local Excel files (`UID NUMBER.xlsx`, `rfid_log.xlsx`, `USERNAME.xlsx`) and a configured repair log file, and writes them into `database.db` (tables replaced).

- `create_sqlite_schema.py`
  - Despite the name, this is also an import script: it reads multiple Excel files from hardcoded `G:\kitkart\New folder\...` paths and writes them into `database.db`.
  - Useful for quickly rebuilding tables from spreadsheets during setup/testing.

- `check_tables.py`
  - Tiny debug utility: connects to `rfid_log.db` and prints the table names.

### Templates (UI)

- `templates/records.html`
  - Main “Records” page rendering `rfid_log` rows, with prefix filtering and links/actions that route to record updates.

- `templates/update_form.html`
  - The detailed edit form for a single RFID log record (`/update_record/<id>`).
  - Supports selecting check points and capturing concerns/actions, plus guardrails based on due-date proximity.

- `templates/dashboard.html`
  - Dashboard view with Chart.js graphs and counters (monthly plan vs actual, duration, repairs, concern checkpoints).

- `templates/repair_log.html`
  - Displays the `repair_log` table.
  - Provides a “Take Action” modal per row; includes an RFID popup to fill the technician name via `/get_username/<rfid>`.

- `templates/reports.html`
  - Summary report page (counts by TPM category and trolley type, plus “Due in next 7 days” table).

- `templates/import_export.html`
  - Export `rfid_log` as Excel via `/download_excel` (optional trolley prefix filter).
  - Repair log sync actions:
    - Sync from configured path (`action=sync_path`)
    - Upload a repair Excel and sync (`action=upload_repair_excel`)

- `templates/settings.html`
  - Allows editing the configured repair Excel path saved in `settings.json`.

- `templates/new_record.html`
  - Manual creation form for a new `rfid_log` row.

- `templates/repair_entry.html`
  - Manual creation form for a new `repair_log` row.

### Static assets

- `static/b.jpg`
  - Background image used by most pages.

- `static/logo.png`
  - Branding image (used on some pages).

### Data and state files

- `database.db`
  - The primary SQLite database used by the Flask app.

- `rfid_log.xlsx`
  - Example/exported Excel file for RFID log data.

- `REPAIR_LOG_LOCAL.xlsx`
  - Example/local repair log Excel file.

- `rfid_log.db`, `trolley_database.db`, `your_database.db`
  - Additional SQLite DB files present in the folder (likely older experiments or alternate datasets).

- `sent_notifications.json`
  - Tracks which due-date notifications have already been sent (to prevent duplicates).

- `alerted_trolleys.json`
  - Tracks which “repair pending action” alerts have been sent (to prevent duplicates).

- `~$REPAIR_LOG_LOCAL.xlsx`
  - Temporary Excel lock file (created by Excel while the workbook is open).

- `__pycache__/`
  - Python bytecode cache folder (auto-generated).

## Notes / configuration caveats

- Email notifications:
  - The due-date email sender in `database.py` references `EMAIL_ADDRESS`, `EMAIL_PASSWORD`, and `RECIPIENT_EMAILS`, but these variables are not defined in the code as checked in. As a result, due-date emailing is expected to error and be skipped unless you add/configure those settings.
  - The repair pending-action email function (`send_trolley_alerts`) is currently defined but not invoked by the app.

- RFID socket server:
  - The TCP server code references a `clients` dictionary (for tracking connected sockets), but `clients` is not defined in the code as checked in. If you enable/use the socket server, you’ll need to add that shared structure or remove those references.

- Paths:
  - Several scripts include hardcoded `G:\...` paths for Excel input. In the running web app, the repair log path is configurable via `settings.json` and the Settings page.
