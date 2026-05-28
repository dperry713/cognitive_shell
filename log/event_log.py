import json
import time
import uuid

class EventLog:
    def __init__(self):
        self.entries = []
        self.committed_index = 0

    def append(self, event_type, payload):
        entry = {
            "id": str(uuid.uuid4()),
            "type": event_type,
            "payload": payload,
            "ts": time.time()
        }

        self.entries.append(entry)
        return entry

    def commit(self, index):
        self.committed_index = index

    def get_committed(self):
        return self.entries[:self.committed_index]
