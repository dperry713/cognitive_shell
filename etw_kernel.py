import time
import sqlite3
from datetime import datetime
from etw import ETW
from db import DB

def log(event_type, data):
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    c.execute("""
    INSERT INTO kernel_events VALUES (NULL,?,?,?)
    """, (datetime.now().isoformat(), event_type, str(data)))

    conn.commit()
    conn.close()

def callback(event):
    d = event.event_data

    if "FileName" in d:
        log("file", d["FileName"])

    if "ImageFileName" in d:
        log("process", d["ImageFileName"])

def run():
    providers = [
        "Microsoft-Windows-Kernel-File",
        "Microsoft-Windows-Kernel-Process"
    ]

    with ETW(providers=providers, event_callback=callback):
        while True:
            time.sleep(1)
