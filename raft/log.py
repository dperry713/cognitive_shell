class RaftLog:
    def __init__(self):
        # We start index at 1. Index 0 is a sentinel.
        self.entries = []
        # Snapshot metadata
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
        # Convert 1-indexed to list index, taking compaction into account
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
        """Removes entries starting from the given index (inclusive)."""
        if index <= self.last_snapshot_index:
            # Cannot truncate compacted part
            return
        list_idx = index - 1 - self.last_snapshot_index
        if 0 <= list_idx < len(self.entries):
            self.entries = self.entries[:list_idx]

    def slice_from(self, index):
        """Returns entries starting from index (inclusive)."""
        if index <= self.last_snapshot_index:
            # If the requested index is in the snapshot, the caller needs all remaining entries
            return self.entries
        list_idx = index - 1 - self.last_snapshot_index
        if 0 <= list_idx < len(self.entries):
            return self.entries[list_idx:]
        return []

    def compact(self, snapshot_index, snapshot_term):
        """Removes entries up to and including snapshot_index."""
        if snapshot_index <= self.last_snapshot_index:
            return False
        
        # Find index in list
        list_idx = snapshot_index - 1 - self.last_snapshot_index
        if list_idx >= len(self.entries):
            # Compacted past the end of our current log, clear all
            self.entries = []
        else:
            self.entries = self.entries[list_idx + 1:]
            
        self.last_snapshot_index = snapshot_index
        self.last_snapshot_term = snapshot_term
        return True
