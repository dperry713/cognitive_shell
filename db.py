import sqlite3

DB = "activity.db"

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY,
        start_time TEXT,
        end_time TEXT,
        process TEXT,
        title TEXT,
        duration REAL,
        category TEXT,
        idle INTEGER
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS kernel_events (
        id INTEGER PRIMARY KEY,
        timestamp TEXT,
        event_type TEXT,
        data TEXT
    )
    """)

    conn.commit()
    conn.close()
