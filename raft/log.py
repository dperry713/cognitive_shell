class RaftLog:
    def __init__(self):
        self.entries = []
        self.last_snapshot_index = 0
        self.last_snapshot_term = 0

    def append(self, term, command):
        next_index = self.last_log_index() + 1
        entry = {
            "term": term,
            "index": next_index,
            "command": command
        }
        self.entries.append(entry)
        return entry

    def get_entry(self, index):
        if index <= self.last_snapshot_index:
            return None
        list_idx = index - 1 - self.last_snapshot_index
        if 0 <= list_idx < len(self.entries):
            return self.entries[list_idx]
        return None

    def last_log_index(self):
        if self.entries:
            return self.entries[-1]["index"]
        return self.last_snapshot_index

    def last_log_term(self):
        if self.entries:
            return self.entries[-1]["term"]
        return self.last_snapshot_term

    def truncate_from(self, index):
        if index <= self.last_snapshot_index:
            return
        list_idx = index - 1 - self.last_snapshot_index
        if 0 <= list_idx < len(self.entries):
            self.entries = self.entries[:list_idx]

    def slice_from(self, index):
        if index <= self.last_snapshot_index:
            return self.entries
        list_idx = index - 1 - self.last_snapshot_index
        if 0 <= list_idx < len(self.entries):
            return self.entries[list_idx:]
        return []

    def compact(self, snapshot_index, snapshot_term):
        if snapshot_index <= self.last_snapshot_index:
            return False
        list_idx = snapshot_index - 1 - self.last_snapshot_index
        if list_idx >= len(self.entries):
            self.entries = []
        else:
            self.entries = self.entries[list_idx + 1:]
        self.last_snapshot_index = snapshot_index
        self.last_snapshot_term = snapshot_term
        return True
