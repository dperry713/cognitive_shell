import threading
import time
import tracker_user
import etw_kernel
from db import init_db

def main():
    init_db()

    t1 = threading.Thread(target=tracker_user.run, daemon=True)
    t2 = threading.Thread(target=etw_kernel.run, daemon=True)

    t1.start()
    t2.start()

    while True:
        time.sleep(10)

if __name__ == "__main__":
    main()
