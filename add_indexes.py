# add_indexes.py
import sqlite3

# Connect to your DB
conn = sqlite3.connect("data/ngx_equities.db")
cursor = conn.cursor()

# Create indexes if they don't exist
cursor.execute("CREATE INDEX IF NOT EXISTS idx_equities_name ON equities(name);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_equities_date ON equities(date);")

# Confirm
print("✅ Indexes created (or already existed).")

# Close connection
conn.commit()
conn.close()
