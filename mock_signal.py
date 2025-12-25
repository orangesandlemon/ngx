import sqlite3
import os

DB_PATH = "data/ngx_equities.db"  # or your backup

try:
    # Step 1: Confirm file exists
    if not os.path.exists(DB_PATH):
        print("❌ DB file not found.")
    else:
        # Step 2: Try real write
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")  # Switch to WAL if not already
            cursor.execute("CREATE TABLE IF NOT EXISTS test_lock_check (id INTEGER);")
            cursor.execute("INSERT INTO test_lock_check (id) VALUES (1);")
            conn.commit()
            print("✅ Database is NOT locked. Write succeeded.")
except sqlite3.OperationalError as e:
    if "locked" in str(e).lower():
        print("🔒 Database is LOCKED:", e)
    else:
        print("❌ Operational error:", e)
except Exception as e:
    print("❌ General error:", e)

