import sqlite3
conn = sqlite3.connect("rfid_log.db")
print(conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall())