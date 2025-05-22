# deduplicate_and_enforce_pk.py
import sqlite3
import os

DB_PATH = "data/ngx_equities.db"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print("🛠 Starting deduplication...")

# Step 1: Create new table with PRIMARY KEY(name, date)
cursor.execute("""
CREATE TABLE IF NOT EXISTS equities_clean (
    name TEXT,
    previous_close REAL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    change REAL,
    trades INTEGER,
    volume INTEGER,
    value REAL,
    date TEXT,
    PRIMARY KEY(name, date)
)
""")

# Step 2: Insert DISTINCT records from old table
cursor.execute("""
INSERT OR IGNORE INTO equities_clean
SELECT DISTINCT * FROM equities
""")

print("✅ Unique records copied to equities_clean")

# Step 3: Backup original table
cursor.execute("ALTER TABLE equities RENAME TO equities_backup")
print("📦 Old table renamed to equities_backup")

# Step 4: Rename clean table to original name
cursor.execute("ALTER TABLE equities_clean RENAME TO equities")
print("✅ Clean table renamed to equities")

# Step 5: Rebuild indexes
cursor.execute("CREATE INDEX IF NOT EXISTS idx_equities_name ON equities(name)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_equities_date ON equities(date)")

conn.commit()
conn.close()
print("🎉 All done! DB is now clean, deduplicated, and protected from future duplicates.")
