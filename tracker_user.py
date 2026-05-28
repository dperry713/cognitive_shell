import sqlite3
import psutil
import win32gui
import win32process
import win32con
from ctypes import windll, wintypes, byref, Structure, c_uint, sizeof
from datetime import datetime
from db import DB

user32 = windll.user32

class LASTINPUTINFO(Structure):
    _fields_ = [("cbSize", c_uint), ("dwTime", c_uint)]

def idle_seconds():
    lii = LASTINPUTINFO()
    lii.cbSize = sizeof(lii)
    user32.GetLastInputInfo(byref(lii))
    return (windll.kernel32.GetTickCount() - lii.dwTime) / 1000.0

def categorize(p, t):
    p = p.lower()
    t = t.lower()

    if "code" in p:
        return "development"
    if "chrome" in p or "edge" in p:
        if "youtube" in t:
            return "entertainment"
        return "browser"
    if "slack" in p or "discord" in p:
        return "communication"
    return "other"

def log_session(s, e, p, t, idle):
    dur = (e - s).total_seconds()
    if dur <= 0:
        return

    conn = sqlite3.connect(DB)
    c = conn.cursor()

    c.execute("""
    INSERT INTO sessions VALUES (NULL,?,?,?,?,?,?,?)
    """, (s.isoformat(), e.isoformat(), p, t, dur, categorize(p, t), int(idle)))

    conn.commit()
    conn.close()

WinProc = wintypes.WINFUNCTYPE(
    None, wintypes.HANDLE, wintypes.DWORD, wintypes.HWND,
    wintypes.LONG, wintypes.LONG, wintypes.DWORD, wintypes.DWORD
)

last = {"p": None, "t": None, "s": None}

def get_window(hwnd):
    title = win32gui.GetWindowText(hwnd)
    try:
        _, pid = win32process.GetWindowThreadProcessId(hwnd)
        proc = psutil.Process(pid).name()
    except:
        proc = "unknown"
    return proc, title

def on_focus(hwnd):
    global last
    p, t = get_window(hwnd)
    now = datetime.now()
    idle = idle_seconds() > 60

    if p != last["p"] or t != last["t"]:
        if last["p"]:
            log_session(last["s"], now, last["p"], last["t"], idle)

        last = {"p": p, "t": t, "s": now}

def callback(h, e, hwnd, *args):
    if hwnd:
        on_focus(hwnd)

def run():
    hook = user32.SetWinEventHook(
        win32con.EVENT_SYSTEM_FOREGROUND,
        win32con.EVENT_SYSTEM_FOREGROUND,
        0,
        WinProc(callback),
        0,
        0,
        win32con.WINEVENT_OUTOFCONTEXT
    )

    msg = wintypes.MSG()
    while True:
        user32.GetMessageW(byref(msg), 0, 0, 0)
        user32.DispatchMessageW(byref(msg))
